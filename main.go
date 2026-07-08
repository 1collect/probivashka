package main

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	baseURL                   = "https://aisoip.adilet.gov.kz/extperson"
	debtorBaseURL             = "https://aisoip.adilet.gov.kz"
	workerCount               = 8
	pdfTitleWorkerCount       = 48
	maxPDFTitleWorkers        = 96
	targetExecProcStatus      = "Окончено"
	targetExecProcStatusCode  = "50"
	targetPDFTitle            = "Постановление о прекращении исполнительного производства"
	targetPDFTitleKZ          = "Қаулы атқарушылық іс жүргізуді тоқтату туралы"
	defaultPDFParseAPI        = "http://127.0.0.1:8890/api/parse-pdf"
	defaultPDFParseAPIWorkers = 4
	maxPDFParseAPIWorkers     = 16
	defaultExecProcStartDate  = "2000-01-01"
	execProcUA                = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Mobile Safari/537.36 Edg/148.0.0.0"
	debtorUA                  = "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36"
	defaultERDCaptcha         = "HFOWZlKh9DWSAcCllQF0xBFggaOy00SA8iSw4PYyAXXgohRzY0XRN4aQZCeCJGLwYiMxYNSFVBO2Z-IBVDG3EPeUJIB2kzSgsUemgkfwI7dXBHKHBgNCcSCwgCJUZNC0hNAgU0M2w7BGIBBltwZExASCVuOXdRezF1GCFkPQ8zPzccXlJBSU5gLlprZWNDPFBeNw1CGyxyF0FiGCcjXXVvGAQiOBNHKE99VwF8ShcUSBcLRhU4amQNeQkPKGtjFFUUemgkfwI1aGpTHic3UDASaB9zRhUWGW92QkgdGjMbGC9HAGxIPEZiLGJxCkBDbRlRW0g7a1xlFSdcCVZLSGlgNQMNOzojOX0wQQx6NW9NCl51UTBXWn5tfgY3IhxAM1ZpVwB9XRUUOUlKBjliPnEPeR0ITjZUXQsBGEJ5KFx2MzQOcnBhRUNQPUFVNEJGM05QVxkTYjZgTyUZRAY2YmxyOVpkeCBwKmw3B209ag8zTFVLD1dLUQIWSG5Ae2drbhsnNQ8DYzsUSAI6RCcjXx0yOV1rb3UbbXBwFFJNEQIWPhIdQ3g4bHEPewkILw"
	maxJobLogLines            = 300
)

var (
	httpClient     *http.Client
	httpClientErr  error
	httpClientOnce sync.Once

	pdfParseAPISemaphore     chan struct{}
	pdfParseAPISemaphoreOnce sync.Once
)

func getHTTPClient() (*http.Client, error) {
	httpClientOnce.Do(func() {
		pool, err := x509.SystemCertPool()
		if err != nil {
			httpClientErr = fmt.Errorf("не удалось загрузить системные сертификаты: %w", err)
			return
		}
		if pool == nil {
			pool = x509.NewCertPool()
		}

		extraCertFile := strings.TrimSpace(os.Getenv("EXTRA_CA_CERT_FILE"))
		if extraCertFile != "" {
			pemData, err := os.ReadFile(extraCertFile)
			if err != nil {
				httpClientErr = fmt.Errorf("не удалось прочитать EXTRA_CA_CERT_FILE=%q: %w", extraCertFile, err)
				return
			}
			if ok := pool.AppendCertsFromPEM(pemData); !ok {
				httpClientErr = fmt.Errorf("файл EXTRA_CA_CERT_FILE=%q не содержит PEM-сертификатов", extraCertFile)
				return
			}
			log.Printf("добавлен дополнительный CA сертификат из %s", extraCertFile)
		}

		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    pool,
		}
		transport.MaxIdleConns = 256
		transport.MaxIdleConnsPerHost = 128
		transport.IdleConnTimeout = 90 * time.Second

		httpClient = &http.Client{
			Timeout:   60 * time.Second,
			Transport: transport,
		}
	})

	return httpClient, httpClientErr
}

var exportColumns = []exportColumn{
	{Header: "Номер исполнительного производства", Path: "execProcNum"},
	{Header: "ИИН/БИН должника", Path: "debtorIinBin"},
	{Header: "ФИО должника", Path: "debtorFullName"},
	{Header: "Сумма долга в ИП в тг", Path: "recoveryAmount"},
	{Header: "Сумма долга в ИП в МРП", Path: "recoveryAmountMrp"},
	{Header: "Частичное погашение: ИП", Path: "collectedInfo.sumColOrder"},
	{Header: "Частичное погашение: Ручной ввод СИ", Path: "collectedInfo.sumManual"},
	{Header: "Частичное погашение: Взыскание с ЗП", Path: "collectedInfo.sumWageRec"},
	{Header: "Частичное погашение: Другие источники", Path: "collectedInfo.sumOther"},
}

var bankArrestColumns = []exportColumn{
	{Header: "Наименование БВУ", Path: "bank.name_ru"},
	{Header: "Уникальный идентификатор счёта", Path: "uniqueAccNumber"},
	{Header: "Последний статус ареста", Path: "arrestStatus.name_ru"},
	{Header: "Дата последнего изменения статуса ареста", Path: "arrestDate"},
	{Header: "Последний статус ИР", Path: "irStatus.name_ru"},
	{Header: "Дата последнего изменения статуса ИР", Path: "irDate"},
}

var notaryBanColumns = []exportColumn{
	{Header: "Статус постановления о наложении ареста", Path: "status.name_ru"},
	{Header: "Дата наложения ареста", Path: "banDate"},
	{Header: "Дата снятия ареста", Path: "unbanDate"},
}

var gcvpColumns = []exportColumn{
	{Header: "Дата", Path: "payDate"},
	{Header: "Сумма", Path: "amount"},
	{Header: "Работодатель", Path: "payerName"},
	{Header: "БИН работодателя", Path: "payerBin"},
}

var driverLicenseColumns = []exportColumn{
	{Header: "Наличие водительских прав", Path: "hasAutoDrDoc"},
	{Header: "Срок действия водительских прав", Path: "expireDate"},
}

var notificationColumns = []exportColumn{
	{Header: "Дата отправки", Path: "statusDate"},
	{Header: "Основание извещения должника", Path: "type.name_ru"},
	{Header: "Канал", Path: "channel.name_ru"},
	{Header: "Статус", Path: "status.name_ru"},
}

var autoInfoColumns = []exportColumn{
	{Header: "Ответ от ТС", Path: "arrestStatus.name_ru"},
	{Header: "Статус постановления ареста", Path: "status.name_ru"},
	{Header: "Количество арестованных ТС", Path: "objCount"},
	{Header: "Дата наложения ареста", Path: "banDate"},
	{Header: "Дата снятия ареста", Path: "unbanDate"},
}

var travelBanColumns = []exportColumn{
	{Header: "Дата извещения должника", Path: "notifDate"},
	{Header: "Статус постановления о наложении запрета", Path: "status.name_ru"},
	{Header: "Статус наложения запрета", Path: "arrestStatus.name_ru"},
	{Header: "Дата наложения запрета", Path: "banDate"},
	{Header: "Дата приостановления запрета", Path: "suspDate"},
	{Header: "Дата снятия запрета", Path: "unbanDate"},
}

var debtorERDColumns = []string{
	"ИИН",
	"Количество результатов",
	"Орган, выдавший исполнительный документ",
	"Номер исполнительного производства и дата возбуждения",
	"Взыскатель",
	"Сумма взысканий",
	"Орган исполнительного пр-ва, судебный исполнитель",
	"Ход производства",
	"Содержание неисполненной обязанности должника",
	"Статус запроса",
	"Ошибка",
}

var registrationBanColumns = []exportColumn{
	{Header: "Статус постановления о наложении ареста", Path: "status.name_ru"},
	{Header: "Дата наложения ареста", Path: "banDate"},
	{Header: "Дата снятия ареста", Path: "unbanDate"},
}

var propertyArrestColumns = []exportColumn{
	{Header: "Ответ от ГБД РН", Path: "arrestStatus.name_ru"},
	{Header: "Статус постановления ареста", Path: "status.name_ru"},
	{Header: "Количество арестованной недвижимости", Path: "objCount"},
	{Header: "Дата наложения ареста", Path: "banDate"},
	{Header: "Дата снятия ареста", Path: "unbanDate"},
}

type exportColumn struct {
	Header string
	Path   string
}

type sheetData struct {
	Name string
	Rows [][]string
}

type xlsxWorksheet struct {
	SheetData struct {
		Rows []xlsxRow `xml:"row"`
	} `xml:"sheetData"`
}

type xlsxRow struct {
	Cells []xlsxCell `xml:"c"`
}

type xlsxCell struct {
	Ref       string `xml:"r,attr"`
	Type      string `xml:"t,attr"`
	Value     string `xml:"v"`
	InlineStr struct {
		Text string `xml:"t"`
	} `xml:"is"`
}

type xlsxSharedStrings struct {
	Items []struct {
		Text string `xml:"t"`
		Runs []struct {
			Text string `xml:"t"`
		} `xml:"r"`
	} `xml:"si"`
}

type startRequest struct {
	Type       string          `json:"type"`
	SessionKey string          `json:"sessionKey"`
	FileName   string          `json:"fileName"`
	FileBase64 string          `json:"fileBase64"`
	SelectAll  bool            `json:"selectAll"`
	Options    map[string]bool `json:"options"`
	OwnerToken string          `json:"ownerToken"`
	JobID      string          `json:"jobId"`
}

type execExportRequest struct {
	Type        string   `json:"type"`
	SessionKey  string   `json:"sessionKey"`
	StartDate   string   `json:"startDate"`
	Statuses    []string `json:"statuses"`
	OutputFile  string   `json:"outputFile"`
	DownloadDir string   `json:"downloadDir"`
	OwnerToken  string   `json:"ownerToken"`
	JobID       string   `json:"jobId"`
}

type pdfTitlesRequest struct {
	Type       string `json:"type"`
	SessionKey string `json:"sessionKey"`
	FileName   string `json:"fileName"`
	FileBase64 string `json:"fileBase64"`
	Workers    int    `json:"workers"`
	OwnerToken string `json:"ownerToken"`
	JobID      string `json:"jobId"`
}

type debtorERDRequest struct {
	Type       string `json:"type"`
	FileName   string `json:"fileName"`
	FileBase64 string `json:"fileBase64"`
	Workers    int    `json:"workers"`
	OwnerToken string `json:"ownerToken"`
	JobID      string `json:"jobId"`
}

type requestStatusRequest struct {
	Type       string `json:"type"`
	SessionKey string `json:"sessionKey"`
	FileName   string `json:"fileName"`
	FileBase64 string `json:"fileBase64"`
	Workers    int    `json:"workers"`
	OwnerToken string `json:"ownerToken"`
	JobID      string `json:"jobId"`
}

type downloadFile struct {
	FileName   string `json:"fileName"`
	FileBase64 string `json:"fileBase64"`
}

type wsMessage struct {
	Type               string         `json:"type"`
	JobID              string         `json:"jobId,omitempty"`
	Message            string         `json:"message,omitempty"`
	Current            int            `json:"current,omitempty"`
	Total              int            `json:"total,omitempty"`
	Number             string         `json:"number,omitempty"`
	Status             string         `json:"status,omitempty"`
	Error              string         `json:"error,omitempty"`
	FileName           string         `json:"fileName,omitempty"`
	FileBase64         string         `json:"fileBase64,omitempty"`
	Processed          int            `json:"processed,omitempty"`
	Failed             int            `json:"failed,omitempty"`
	Logs               []string       `json:"logs,omitempty"`
	ExtraFiles         []downloadFile `json:"extraFiles,omitempty"`
	LocalUnhandledFile string         `json:"localUnhandledFile,omitempty"`
}

type httpStatusError struct {
	StatusCode int
	Body       string
}

func (e httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, strings.TrimSpace(e.Body))
}

type unhandledEntry struct {
	ExecProcNum     string         `json:"execProcNum"`
	DebtorFullName  string         `json:"debtorFullName"`
	DebtorIinBin    string         `json:"debtorIinBin"`
	UnhandledBlocks map[string]any `json:"unhandledBlocks"`
}

type job struct {
	Index  int
	Number string
}

type pdfParseJob struct {
	Index       int
	RowIndex    int
	Number      string
	ExecProcNum string
}

type fetchResult struct {
	Index     int
	Number    string
	Parsed    map[string]any
	Err       error
	ResultRow []string
	Unhandled *unhandledEntry
}

type pdfTitleResult struct {
	Index             int
	RowIndex          int
	Number            string
	ExecProcNum       string
	ExecProcID        string
	AllTitles         []string
	Titles            []string
	PDFRefs           []string
	DownloadedPDF     string
	PDFSHA1           string
	ParserFileSHA1    string
	ParserTextSHA1    string
	ParserPageCount   int
	ParserTextPreview string
	DecreeDate        string
	LegalBasis        string
	ExecProcCount     int
	Err               error
}

type matchedPDFDocument struct {
	Title string
	Doc   map[string]any
	Lang  string
}

type debtorERDJob struct {
	Index int
	IIN   string
}

type debtorERDResult struct {
	Index       int
	IIN         string
	Total       int
	DetailRows  [][]string
	DetailCount int
	Err         error
}

type requestStatusJob struct {
	Index  int
	Number string
}

type requestStatusEntry struct {
	Request      string `json:"request"`
	State        string `json:"state"`
	CreatedDate  string `json:"createdDate"`
	ResponseDate string `json:"responseDate"`
}

type requestStatusResult struct {
	Index         int
	Number        string
	ExecProcCount int
	Pension       requestStatusEntry
	Benefit       requestStatusEntry
	Err           error
}

type pdfJobState struct {
	ID          string
	OwnerToken  string
	Status      string
	Canceled    bool
	Paused      bool
	SessionKey  string
	Current     int
	Total       int
	Number      string
	Processed   int
	Failed      int
	FileName    string
	FileBase64  string
	Message     string
	Error       string
	Logs        []string
	StartedAt   time.Time
	CompletedAt time.Time
}

var (
	pdfJobs     = map[string]*pdfJobState{}
	pdfJobsLock sync.Mutex
)

func main() {
	addr := serverAddr()

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/execproc-ws", handleExecProcWebSocket)
	http.HandleFunc("/pdf-titles-ws", handlePDFTitlesWebSocket)
	http.HandleFunc("/pdf-titles-v2-ws", handlePDFTitlesUpdatedWebSocket)
	http.HandleFunc("/debtor-erd-ws", handleDebtorERDWebSocket)
	http.HandleFunc("/request-status-ws", handleRequestStatusWebSocket)

	log.Printf("HTTP server started on http://localhost%s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

func serverAddr() string {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8888"
	}
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}
	return port
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, "index.html")
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	payload, err := readClientTextFrame(conn)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	var req startRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось разобрать запрос"})
		return
	}

	if req.Type == "check-watch" {
		watchPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "check-cancel" {
		cancelPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "check-token" {
		updatePDFJobSession(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken), strings.TrimSpace(req.SessionKey))
		return
	}

	ownerToken := strings.TrimSpace(req.OwnerToken)
	if ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "ownerToken обязателен"})
		return
	}

	if strings.TrimSpace(req.SessionKey) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "ключ сессии обязателен"})
		return
	}
	if strings.TrimSpace(req.FileBase64) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "файл не был передан"})
		return
	}

	fileBytes, err := base64.StdEncoding.DecodeString(req.FileBase64)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось декодировать файл"})
		return
	}

	numbers, err := readNumbersFromXLSXBytes(fileBytes)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if len(numbers) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "в загруженном xlsx не найдено ни одного номера"})
		return
	}

	jobID := newJobID()
	createPDFJob(jobID, ownerToken, len(numbers), "Подготовка", req.SessionKey)
	_ = writeServerJSON(conn, wsMessage{Type: "job", JobID: jobID, Total: len(numbers), Message: "Задача запущена"})

	rows := make([][]string, 0, len(numbers)+1)
	header := make([]string, 0, len(exportColumns)+2)
	for _, column := range exportColumns {
		header = append(header, column.Header)
	}
	header = append(header, "Статус запроса", "Ошибка")
	rows = append(rows, header)

	includeBankArrest := req.SelectAll || req.Options["bankArrest"]
	bankArrestRows := make([][]string, 0)
	if includeBankArrest {
		bankHeader := make([]string, 0, len(bankArrestColumns)+3)
		bankHeader = append(bankHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range bankArrestColumns {
			bankHeader = append(bankHeader, column.Header)
		}
		bankArrestRows = append(bankArrestRows, bankHeader)
	}

	includeNotaryBan := req.SelectAll || req.Options["notaryBan"]
	notaryBanRows := make([][]string, 0)
	if includeNotaryBan {
		notaryHeader := make([]string, 0, len(notaryBanColumns)+3)
		notaryHeader = append(notaryHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range notaryBanColumns {
			notaryHeader = append(notaryHeader, column.Header)
		}
		notaryBanRows = append(notaryBanRows, notaryHeader)
	}

	includeGCVP := req.SelectAll || req.Options["gcvpPayments"]
	gcvpRows := make([][]string, 0)
	if includeGCVP {
		gcvpHeader := make([]string, 0, len(gcvpColumns)+4)
		gcvpHeader = append(gcvpHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника", "Категория")
		for _, column := range gcvpColumns {
			gcvpHeader = append(gcvpHeader, column.Header)
		}
		gcvpRows = append(gcvpRows, gcvpHeader)
	}

	includeDriverLicense := req.SelectAll || req.Options["driverLicense"]
	driverLicenseRows := make([][]string, 0)
	if includeDriverLicense {
		driverHeader := make([]string, 0, len(driverLicenseColumns)+3)
		driverHeader = append(driverHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range driverLicenseColumns {
			driverHeader = append(driverHeader, column.Header)
		}
		driverLicenseRows = append(driverLicenseRows, driverHeader)
	}

	includeNotifications := req.SelectAll || req.Options["notificationMethod"]
	notificationRows := make([][]string, 0)
	if includeNotifications {
		notificationHeader := make([]string, 0, len(notificationColumns)+3)
		notificationHeader = append(notificationHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range notificationColumns {
			notificationHeader = append(notificationHeader, column.Header)
		}
		notificationRows = append(notificationRows, notificationHeader)
	}

	includeAutoInfo := req.SelectAll || req.Options["transportArrest"]
	autoInfoRows := make([][]string, 0)
	if includeAutoInfo {
		autoInfoHeader := make([]string, 0, len(autoInfoColumns)+3)
		autoInfoHeader = append(autoInfoHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range autoInfoColumns {
			autoInfoHeader = append(autoInfoHeader, column.Header)
		}
		autoInfoRows = append(autoInfoRows, autoInfoHeader)
	}

	includeTravelBan := req.SelectAll || req.Options["travelBan"]
	travelBanRows := make([][]string, 0)
	if includeTravelBan {
		travelBanHeader := make([]string, 0, len(travelBanColumns)+3)
		travelBanHeader = append(travelBanHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range travelBanColumns {
			travelBanHeader = append(travelBanHeader, column.Header)
		}
		travelBanRows = append(travelBanRows, travelBanHeader)
	}

	includeRegistrationBan := req.SelectAll || req.Options["registrationBan"]
	registrationBanRows := make([][]string, 0)
	if includeRegistrationBan {
		registrationHeader := make([]string, 0, len(registrationBanColumns)+3)
		registrationHeader = append(registrationHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range registrationBanColumns {
			registrationHeader = append(registrationHeader, column.Header)
		}
		registrationBanRows = append(registrationBanRows, registrationHeader)
	}

	includePropertyArrest := req.SelectAll || req.Options["propertyArrest"]
	propertyArrestRows := make([][]string, 0)
	if includePropertyArrest {
		propertyHeader := make([]string, 0, len(propertyArrestColumns)+3)
		propertyHeader = append(propertyHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range propertyArrestColumns {
			propertyHeader = append(propertyHeader, column.Header)
		}
		propertyArrestRows = append(propertyArrestRows, propertyHeader)
	}

	processed := 0
	failed := 0
	unhandledEntries := make([]unhandledEntry, 0)

	jobs := make(chan job)
	resultsCh := make(chan fetchResult, len(numbers))

	for i := 0; i < workerCount; i++ {
		go func() {
			for currentJob := range jobs {
				if isPDFJobCanceled(jobID) {
					resultsCh <- fetchResult{Index: currentJob.Index, Number: currentJob.Number, Err: errors.New("операция отменена")}
					continue
				}
				resultsCh <- processNumberWithAuth(jobID, currentJob.Index, currentJob.Number, header)
			}
		}()
	}

	go func() {
		for idx, number := range numbers {
			jobs <- job{Index: idx, Number: number}
		}
		close(jobs)
	}()

	results := make([]fetchResult, len(numbers))
	progressTicker := time.NewTicker(time.Second)
	defer progressTicker.Stop()
	lastAuthSignature := ""
	for i := 0; i < len(numbers); {
		var result fetchResult
		select {
		case result = <-resultsCh:
			i++
		case <-progressTicker.C:
			message, signature, _, ok := pdfJobSnapshot(jobID, ownerToken)
			if ok && message.Type == "auth" && signature != lastAuthSignature {
				_ = writeServerJSON(conn, message)
				lastAuthSignature = signature
			}
			continue
		}
		results[result.Index] = result
		if isPDFJobCanceled(jobID) {
			continue
		}
		lastAuthSignature = ""
		progressMessage := wsMessage{
			Type:    "progress",
			JobID:   jobID,
			Current: i + 1,
			Total:   len(numbers),
			Number:  result.Number,
			Status:  statusFromError(result.Err),
			Error:   errorText(result.Err),
		}
		updatePDFJob(jobID, func(job *pdfJobState) {
			job.Current = progressMessage.Current
			job.Number = progressMessage.Number
			job.Message = progressMessage.Status
			job.Error = progressMessage.Error
		})
		_ = writeServerJSON(conn, progressMessage)
	}
	if isPDFJobCanceled(jobID) {
		return
	}

	for _, result := range results {
		rows = append(rows, result.ResultRow)
		if result.Err != nil {
			failed++
			continue
		}

		processed++
		if includeBankArrest {
			bankArrestRows = appendBankArrestRows(bankArrestRows, result.Parsed, result.Number)
		}
		if includeNotaryBan {
			notaryBanRows = appendClEnisRows(notaryBanRows, result.Parsed, result.Number)
		}
		if includeGCVP {
			gcvpRows = appendGCVPRows(gcvpRows, result.Parsed, result.Number)
		}
		if includeDriverLicense {
			driverLicenseRows = appendDriverLicenseRows(driverLicenseRows, result.Parsed, result.Number)
		}
		if includeNotifications {
			notificationRows = appendNotificationRows(notificationRows, result.Parsed, result.Number)
		}
		if includeAutoInfo {
			autoInfoRows = appendAutoInfoRows(autoInfoRows, result.Parsed, result.Number)
		}
		if includeTravelBan {
			travelBanRows = appendTravelBanRows(travelBanRows, result.Parsed, result.Number)
		}
		if includeRegistrationBan {
			registrationBanRows = appendRegistrationBanRows(registrationBanRows, result.Parsed, result.Number)
		}
		if includePropertyArrest {
			propertyArrestRows = appendPropertyArrestRows(propertyArrestRows, result.Parsed, result.Number)
		}
		if result.Unhandled != nil {
			unhandledEntries = append(unhandledEntries, *result.Unhandled)
		}
	}

	sheets := []sheetData{{Name: "Результаты", Rows: rows}}
	if includeGCVP {
		sheets = append(sheets, sheetData{Name: "Выплаты/Пенсионные отчисления", Rows: gcvpRows})
	}
	if includeBankArrest {
		sheets = append(sheets, sheetData{Name: "Арест на банковские счета", Rows: bankArrestRows})
	}
	if includeTravelBan {
		sheets = append(sheets, sheetData{Name: "Временное ограничение на выезд", Rows: travelBanRows})
	}
	if includeAutoInfo {
		sheets = append(sheets, sheetData{Name: "Арест на транспорт", Rows: autoInfoRows})
	}
	if includePropertyArrest {
		sheets = append(sheets, sheetData{Name: "Арест на имущество", Rows: propertyArrestRows})
	}
	if includeNotaryBan {
		sheets = append(sheets, sheetData{Name: "Запрет на совершение нотариальных действий", Rows: notaryBanRows})
	}
	if includeRegistrationBan {
		sheets = append(sheets, sheetData{Name: "Запрет на регистрационные действия", Rows: registrationBanRows})
	}
	if includeDriverLicense {
		sheets = append(sheets, sheetData{Name: "Водительское удостоверение", Rows: driverLicenseRows})
	}
	if includeNotifications {
		sheets = append(sheets, sheetData{Name: "Способ уведомления должника (СМС/ЕТУ)", Rows: notificationRows})
	}

	xlsxBytes, err := buildXLSXBytes(sheets)
	if err != nil {
		finishPDFJobError(jobID, err.Error())
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	resultMessage := wsMessage{
		Type:       "result",
		JobID:      jobID,
		FileName:   datedXLSXFileName("results"),
		FileBase64: base64.StdEncoding.EncodeToString(xlsxBytes),
		Processed:  processed,
		Failed:     failed,
	}
	if len(unhandledEntries) > 0 {
		payload, err := json.MarshalIndent(unhandledEntries, "", "  ")
		if err != nil {
			_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
			return
		}
		localFileName := buildUnhandledFileName()
		if err := os.WriteFile(localFileName, payload, 0644); err != nil {
			_ = writeServerJSON(conn, wsMessage{Type: "error", Message: fmt.Sprintf("не удалось сохранить локальный unhandled json: %v", err)})
			return
		}
		resultMessage.LocalUnhandledFile = localFileName
	}

	finishPDFJobResult(jobID, resultMessage.FileName, resultMessage.FileBase64, processed, failed)
	_ = writeServerJSON(conn, resultMessage)
}

func handlePDFTitlesWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	payload, err := readClientTextFrame(conn)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	var req pdfTitlesRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось разобрать запрос"})
		return
	}

	if req.Type == "pdf-titles-watch" {
		watchPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "pdf-titles-cancel" {
		cancelPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "pdf-titles-token" {
		updatePDFJobSession(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken), strings.TrimSpace(req.SessionKey))
		return
	}

	ownerToken := strings.TrimSpace(req.OwnerToken)
	if ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "ownerToken обязателен"})
		return
	}

	session := strings.TrimSpace(req.SessionKey)
	if session == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "SESSION обязателен"})
		return
	}
	if strings.Contains(session, ";") || strings.Contains(session, "=") {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "вставьте только значение SESSION без дополнительных параметров"})
		return
	}
	if strings.TrimSpace(req.FileBase64) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "файл не был передан"})
		return
	}

	fileBytes, err := base64.StdEncoding.DecodeString(req.FileBase64)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось декодировать файл"})
		return
	}

	sourceRows, err := readXLSXRowsBytes(fileBytes)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	jobsToRun := pdfParseJobsFromRows(sourceRows)
	if len(jobsToRun) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "В XLSX не найдено номеров исполнительных производств в первой колонке."})
		return
	}

	jobID := newJobID()
	workers := normalizePDFTitleWorkers(req.Workers, len(jobsToRun))
	createPDFJob(jobID, ownerToken, len(jobsToRun), fmt.Sprintf("Запущено потоков: %d", workers), session)
	_ = writeServerJSON(conn, wsMessage{Type: "job", JobID: jobID, Total: len(jobsToRun), Message: fmt.Sprintf("Запущено потоков: %d", workers)})

	go runPDFJob(jobID, sourceRows, jobsToRun, workers)
	watchPDFJob(conn, jobID, ownerToken)
}

func handlePDFTitlesUpdatedWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	payload, err := readClientTextFrame(conn)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	var req pdfTitlesRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось разобрать запрос"})
		return
	}

	if req.Type == "pdf-titles-v2-watch" {
		watchPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "pdf-titles-v2-cancel" {
		cancelPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "pdf-titles-v2-token" {
		updatePDFJobSession(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken), strings.TrimSpace(req.SessionKey))
		return
	}

	ownerToken := strings.TrimSpace(req.OwnerToken)
	if ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "ownerToken обязателен"})
		return
	}

	session := strings.TrimSpace(req.SessionKey)
	if session == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "SESSION обязателен"})
		return
	}
	if strings.Contains(session, ";") || strings.Contains(session, "=") {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "вставьте только значение SESSION без дополнительных параметров"})
		return
	}
	if strings.TrimSpace(req.FileBase64) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "файл не был передан"})
		return
	}

	fileBytes, err := base64.StdEncoding.DecodeString(req.FileBase64)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось декодировать файл"})
		return
	}

	sourceRows, err := readXLSXRowsBytes(fileBytes)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	jobsToRun, err := strictPDFParseJobsFromRows(sourceRows)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if len(jobsToRun) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "В XLSX не найдено строк с исполнительным документом и исполнительным производством."})
		return
	}

	jobID := newJobID()
	workers := normalizePDFTitleWorkers(req.Workers, len(jobsToRun))
	createPDFJob(jobID, ownerToken, len(jobsToRun), fmt.Sprintf("Запущено потоков: %d", workers), session)
	_ = writeServerJSON(conn, wsMessage{Type: "job", JobID: jobID, Total: len(jobsToRun), Message: fmt.Sprintf("Запущено потоков: %d", workers)})

	go runPDFUpdatedJob(jobID, sourceRows, jobsToRun, workers)
	watchPDFJob(conn, jobID, ownerToken)
}

func runPDFJob(jobID string, sourceRows [][]string, jobsToRun []pdfParseJob, workers int) {
	jobs := make(chan pdfParseJob)
	resultsCh := make(chan pdfTitleResult, len(jobsToRun))

	for i := 0; i < workers; i++ {
		go func() {
			for currentJob := range jobs {
				if isPDFJobCanceled(jobID) {
					resultsCh <- pdfTitleResult{Index: currentJob.Index, RowIndex: currentJob.RowIndex, Number: currentJob.Number, Err: errors.New("операция отменена")}
					continue
				}
				resultsCh <- fetchPDFTitlesForNumberWithAuth(jobID, currentJob.Index, currentJob.RowIndex, currentJob.Number)
			}
		}()
	}

	go func() {
		for _, currentJob := range jobsToRun {
			jobs <- currentJob
		}
		close(jobs)
	}()

	results := make([]pdfTitleResult, len(jobsToRun))
	documents := 0
	failed := 0
	foundProc := 0
	for i := 0; i < len(jobsToRun); i++ {
		result := <-resultsCh
		results[result.Index] = result
		if isPDFJobCanceled(jobID) {
			continue
		}
		if result.Err != nil {
			failed++
		} else {
			documents += len(result.Titles)
			foundProc += result.ExecProcCount
		}
		updatePDFJob(jobID, func(job *pdfJobState) {
			job.Status = "running"
			job.Current = i + 1
			job.Number = result.Number
			job.Processed = documents
			job.Failed = failed
			job.Message = statusFromError(result.Err)
			job.Error = errorText(result.Err)
		})
	}
	if isPDFJobCanceled(jobID) {
		return
	}

	outputRows := appendPDFParseColumns(sourceRows)
	for _, result := range results {
		setPDFParseColumns(outputRows, result.RowIndex, result.DecreeDate, result.LegalBasis, pdfParseStatus(result))
	}
	xlsxBytes, err := buildXLSXBytes([]sheetData{{Name: "Результаты", Rows: outputRows}})
	if err != nil {
		finishPDFJobError(jobID, err.Error())
		return
	}

	finishPDFJobResult(jobID, datedXLSXFileName("execproc_with_pdf_parse"), base64.StdEncoding.EncodeToString(xlsxBytes), foundProc, failed)
}

func runPDFUpdatedJob(jobID string, sourceRows [][]string, jobsToRun []pdfParseJob, workers int) {
	jobs := make(chan pdfParseJob)
	resultsCh := make(chan pdfTitleResult, len(jobsToRun))

	for i := 0; i < workers; i++ {
		go func() {
			for currentJob := range jobs {
				if isPDFJobCanceled(jobID) {
					resultsCh <- pdfTitleResult{Index: currentJob.Index, RowIndex: currentJob.RowIndex, Number: currentJob.Number, ExecProcNum: currentJob.ExecProcNum, Err: errors.New("операция отменена")}
					continue
				}
				resultsCh <- fetchPDFTitlesForExactExecProcWithAuth(jobID, currentJob.Index, currentJob.RowIndex, currentJob.Number, currentJob.ExecProcNum)
			}
		}()
	}

	go func() {
		for _, currentJob := range jobsToRun {
			jobs <- currentJob
		}
		close(jobs)
	}()

	results := make([]pdfTitleResult, len(jobsToRun))
	documents := 0
	failed := 0
	foundProc := 0
	for i := 0; i < len(jobsToRun); i++ {
		result := <-resultsCh
		results[result.Index] = result
		if isPDFJobCanceled(jobID) {
			continue
		}
		if result.Err != nil {
			failed++
		} else {
			documents += len(result.Titles)
			foundProc += result.ExecProcCount
		}
		updatePDFJob(jobID, func(job *pdfJobState) {
			job.Status = "running"
			job.Current = i + 1
			job.Number = joinNonEmpty(" / ", result.Number, result.ExecProcNum, result.ExecProcID)
			job.Processed = documents
			job.Failed = failed
			job.Message = statusFromError(result.Err)
			job.Error = errorText(result.Err)
		})
	}
	if isPDFJobCanceled(jobID) {
		return
	}

	outputRows := appendPDFUpdatedParseColumns(sourceRows)
	for _, result := range results {
		setPDFUpdatedParseColumns(outputRows, result.RowIndex, result.DecreeDate, result.LegalBasis)
	}
	xlsxBytes, err := buildXLSXBytes([]sheetData{{Name: "Результаты", Rows: outputRows}})
	if err != nil {
		finishPDFJobError(jobID, err.Error())
		return
	}

	finishPDFJobResult(jobID, datedXLSXFileName("execproc_with_pdf_parse_updated"), base64.StdEncoding.EncodeToString(xlsxBytes), foundProc, failed)
}

func handleDebtorERDWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	payload, err := readClientTextFrame(conn)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	var req debtorERDRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось разобрать запрос"})
		return
	}

	if req.Type == "debtor-erd-watch" {
		watchPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "debtor-erd-cancel" {
		cancelPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}

	ownerToken := strings.TrimSpace(req.OwnerToken)
	if ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "ownerToken обязателен"})
		return
	}
	if strings.TrimSpace(req.FileBase64) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "файл не был передан"})
		return
	}

	fileBytes, err := base64.StdEncoding.DecodeString(req.FileBase64)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось декодировать файл"})
		return
	}

	iins, err := readIINsFromXLSXBytes(fileBytes)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if len(iins) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "в загруженном xlsx не найдено ни одного ИИН"})
		return
	}

	jobID := newJobID()
	workers := normalizeDebtorERDWorkers(req.Workers, len(iins))
	createPDFJob(jobID, ownerToken, len(iins), fmt.Sprintf("Запущено потоков: %d", workers), "")
	_ = writeServerJSON(conn, wsMessage{Type: "job", JobID: jobID, Total: len(iins), Message: fmt.Sprintf("Запущено потоков: %d", workers)})

	go runDebtorERDJob(jobID, iins, workers)
	watchPDFJob(conn, jobID, ownerToken)
}

func runDebtorERDJob(jobID string, iins []string, workers int) {
	jobs := make(chan debtorERDJob)
	resultsCh := make(chan debtorERDResult, len(iins))

	for i := 0; i < workers; i++ {
		go func() {
			for currentJob := range jobs {
				if isPDFJobCanceled(jobID) {
					resultsCh <- debtorERDResult{Index: currentJob.Index, IIN: currentJob.IIN, Err: errors.New("операция отменена")}
					continue
				}
				resultsCh <- fetchDebtorERDForIIN(currentJob.Index, currentJob.IIN)
			}
		}()
	}

	go func() {
		for idx, iin := range iins {
			jobs <- debtorERDJob{Index: idx, IIN: iin}
		}
		close(jobs)
	}()

	results := make([]debtorERDResult, len(iins))
	processed := 0
	failed := 0
	details := 0
	for i := 0; i < len(iins); i++ {
		result := <-resultsCh
		results[result.Index] = result
		if isPDFJobCanceled(jobID) {
			continue
		}
		if result.Err != nil {
			failed++
		} else {
			processed++
			details += result.DetailCount
		}
		updatePDFJob(jobID, func(job *pdfJobState) {
			job.Status = "running"
			job.Current = i + 1
			job.Number = result.IIN
			job.Processed = details
			job.Failed = failed
			job.Message = statusFromError(result.Err)
			job.Error = errorText(result.Err)
		})
	}
	if isPDFJobCanceled(jobID) {
		return
	}

	rows := [][]string{debtorERDColumns}
	for _, result := range results {
		if result.Err != nil {
			rows = append(rows, []string{result.IIN, strconv.Itoa(result.Total), "", "", "", "", "", "", "", "Ошибка", result.Err.Error()})
			continue
		}
		if len(result.DetailRows) == 0 {
			rows = append(rows, []string{result.IIN, strconv.Itoa(result.Total), "", "", "", "", "", "", "", "OK", ""})
			continue
		}
		rows = append(rows, result.DetailRows...)
	}

	xlsxBytes, err := buildXLSXBytes([]sheetData{{Name: "Результаты", Rows: rows}})
	if err != nil {
		finishPDFJobError(jobID, err.Error())
		return
	}
	finishPDFJobResult(jobID, datedXLSXFileName("debtor_erd_details"), base64.StdEncoding.EncodeToString(xlsxBytes), details, failed)
	_ = processed
}

func handleRequestStatusWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	payload, err := readClientTextFrame(conn)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	var req requestStatusRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось разобрать запрос"})
		return
	}

	if req.Type == "request-status-watch" {
		watchPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "request-status-cancel" {
		cancelPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "request-status-token" {
		updatePDFJobSession(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken), strings.TrimSpace(req.SessionKey))
		return
	}

	ownerToken := strings.TrimSpace(req.OwnerToken)
	if ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "ownerToken обязателен"})
		return
	}

	session := strings.TrimSpace(req.SessionKey)
	if session == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "SESSION обязателен"})
		return
	}
	if strings.TrimSpace(req.FileBase64) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "файл не был передан"})
		return
	}

	fileBytes, err := base64.StdEncoding.DecodeString(req.FileBase64)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось декодировать файл"})
		return
	}

	numbers, err := readNumbersFromXLSXBytes(fileBytes)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if len(numbers) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "в загруженном xlsx не найдено ни одного номера исполнительного документа"})
		return
	}

	jobID := newJobID()
	workers := normalizeRequestStatusWorkers(req.Workers, len(numbers))
	createPDFJob(jobID, ownerToken, len(numbers), fmt.Sprintf("Запущено потоков: %d", workers), session)
	_ = writeServerJSON(conn, wsMessage{Type: "job", JobID: jobID, Total: len(numbers), Message: fmt.Sprintf("Запущено потоков: %d", workers)})

	go runRequestStatusJob(jobID, numbers, workers)
	watchPDFJob(conn, jobID, ownerToken)
}

func runRequestStatusJob(jobID string, numbers []string, workers int) {
	jobs := make(chan requestStatusJob)
	resultsCh := make(chan requestStatusResult, len(numbers))

	for i := 0; i < workers; i++ {
		go func() {
			for currentJob := range jobs {
				if isPDFJobCanceled(jobID) {
					resultsCh <- requestStatusResult{Index: currentJob.Index, Number: currentJob.Number, Err: errors.New("операция отменена")}
					continue
				}
				resultsCh <- fetchRequestStatusForNumberWithAuth(jobID, currentJob.Index, currentJob.Number)
			}
		}()
	}

	go func() {
		for idx, number := range numbers {
			jobs <- requestStatusJob{Index: idx, Number: number}
		}
		close(jobs)
	}()

	results := make([]requestStatusResult, len(numbers))
	processed := 0
	failed := 0
	foundProc := 0
	for i := 0; i < len(numbers); i++ {
		result := <-resultsCh
		results[result.Index] = result
		if isPDFJobCanceled(jobID) {
			continue
		}
		if result.Err != nil {
			failed++
		} else {
			processed++
			foundProc += result.ExecProcCount
		}
		updatePDFJob(jobID, func(job *pdfJobState) {
			job.Status = "running"
			job.Current = i + 1
			job.Number = result.Number
			job.Processed = foundProc
			job.Failed = failed
			job.Message = statusFromError(result.Err)
			job.Error = errorText(result.Err)
		})
	}
	if isPDFJobCanceled(jobID) {
		return
	}

	rows := [][]string{{
		"Исполнительный документ",
		"Запрос ОПВ",
		"Статус ОПВ",
		"Дата ответа ОПВ",
		"Запрос пенсий и пособий",
		"Статус пенсий и пособий",
		"Дата ответа пенсий и пособий",
	}}
	for _, result := range results {
		row := []string{
			result.Number,
			zeroIfEmpty(result.Pension.Request),
			zeroIfEmpty(result.Pension.State),
			zeroIfEmpty(formatDisplayDate(result.Pension.CreatedDate)),
			zeroIfEmpty(result.Benefit.Request),
			zeroIfEmpty(result.Benefit.State),
			zeroIfEmpty(formatDisplayDate(result.Benefit.CreatedDate)),
		}
		rows = append(rows, row)
	}

	xlsxBytes, err := buildXLSXBytes([]sheetData{{Name: "Результаты", Rows: rows}})
	if err != nil {
		finishPDFJobError(jobID, err.Error())
		return
	}
	finishPDFJobResult(jobID, datedXLSXFileName("execdoc_request_statuses"), base64.StdEncoding.EncodeToString(xlsxBytes), processed, failed)
}

func normalizeRequestStatusWorkers(value int, total int) int {
	if value <= 0 {
		value = workerCount
	}
	if value > 32 {
		value = 32
	}
	if total > 0 && value > total {
		value = total
	}
	if value < 1 {
		value = 1
	}
	return value
}

func zeroIfEmpty(value string) string {
	if strings.TrimSpace(value) == "" {
		return "0"
	}
	return value
}

func normalizeDebtorERDWorkers(value int, total int) int {
	if value <= 0 {
		value = workerCount
	}
	if value > 32 {
		value = 32
	}
	if total > 0 && value > total {
		value = total
	}
	if value < 1 {
		value = 1
	}
	return value
}

func createPDFJob(jobID, ownerToken string, total int, message string, sessionKey string) {
	pdfJobsLock.Lock()
	defer pdfJobsLock.Unlock()
	pdfJobs[jobID] = &pdfJobState{
		ID:         jobID,
		OwnerToken: ownerToken,
		Status:     "running",
		SessionKey: strings.TrimSpace(sessionKey),
		Total:      total,
		Message:    message,
		StartedAt:  time.Now(),
	}
}

func updatePDFJob(jobID string, update func(*pdfJobState)) {
	pdfJobsLock.Lock()
	defer pdfJobsLock.Unlock()
	if job := pdfJobs[jobID]; job != nil {
		update(job)
	}
}

func appendPDFJobLog(jobID, message string) {
	updatePDFJob(jobID, func(job *pdfJobState) {
		job.Message = message
		job.Logs = append(job.Logs, message)
		if len(job.Logs) > maxJobLogLines {
			job.Logs = append([]string(nil), job.Logs[len(job.Logs)-maxJobLogLines:]...)
		}
	})
}

func finishPDFJobError(jobID, message string) {
	updatePDFJob(jobID, func(job *pdfJobState) {
		job.Status = "error"
		job.Error = message
		job.Message = message
		job.Logs = append(job.Logs, message)
		if len(job.Logs) > maxJobLogLines {
			job.Logs = append([]string(nil), job.Logs[len(job.Logs)-maxJobLogLines:]...)
		}
		job.CompletedAt = time.Now()
	})
}

func finishPDFJobResult(jobID, fileName, fileBase64 string, processed, failed int) {
	updatePDFJob(jobID, func(job *pdfJobState) {
		if job.Canceled || job.Paused {
			return
		}
		job.Status = "done"
		job.Current = job.Total
		job.FileName = fileName
		job.FileBase64 = fileBase64
		job.Processed = processed
		job.Failed = failed
		job.Message = "Готово"
		job.CompletedAt = time.Now()
	})
}

func updatePDFJobSession(conn io.Writer, jobID, ownerToken, sessionKey string) {
	if jobID == "" || ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "jobId и ownerToken обязательны"})
		return
	}
	if sessionKey == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "Введите новый SESSION"})
		return
	}
	if strings.Contains(sessionKey, ";") || strings.Contains(sessionKey, "=") {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "вставьте только значение SESSION без дополнительных параметров"})
		return
	}

	pdfJobsLock.Lock()
	job := pdfJobs[jobID]
	if job == nil || job.OwnerToken != ownerToken {
		pdfJobsLock.Unlock()
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "задача не найдена или принадлежит другому пользователю"})
		return
	}
	if job.Canceled {
		pdfJobsLock.Unlock()
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "задача уже отменена"})
		return
	}
	job.SessionKey = sessionKey
	job.Paused = false
	if job.Status == "paused" {
		job.Status = "running"
	}
	job.Message = "SESSION обновлен, продолжаю"
	job.Error = ""
	pdfJobsLock.Unlock()

	_ = writeServerJSON(conn, wsMessage{Type: "token", JobID: jobID, Message: "SESSION обновлен, продолжаю"})
}

func pausePDFJobForAuth(jobID string) {
	updatePDFJob(jobID, func(job *pdfJobState) {
		if job.Canceled || job.Status == "done" || job.Status == "error" {
			return
		}
		job.Status = "paused"
		job.Paused = true
		job.Message = "SESSION больше не валиден. Обновите токен, чтобы продолжить."
		job.Error = ""
	})
}

func waitForActiveJobSession(jobID string) (string, error) {
	for {
		pdfJobsLock.Lock()
		job := pdfJobs[jobID]
		if job == nil {
			pdfJobsLock.Unlock()
			return "", errors.New("задача не найдена")
		}
		if job.Canceled {
			pdfJobsLock.Unlock()
			return "", errors.New("операция отменена")
		}
		if !job.Paused && strings.TrimSpace(job.SessionKey) != "" {
			session := job.SessionKey
			pdfJobsLock.Unlock()
			return session, nil
		}
		pdfJobsLock.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

func cancelPDFJob(conn io.Writer, jobID, ownerToken string) {
	if jobID == "" || ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "jobId и ownerToken обязательны"})
		return
	}

	pdfJobsLock.Lock()
	job := pdfJobs[jobID]
	if job == nil || job.OwnerToken != ownerToken {
		pdfJobsLock.Unlock()
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "задача не найдена или принадлежит другому пользователю"})
		return
	}
	job.Canceled = true
	job.Status = "error"
	job.Message = "Операция отменена"
	job.Error = "Операция отменена"
	job.CompletedAt = time.Now()
	pdfJobsLock.Unlock()

	_ = writeServerJSON(conn, wsMessage{Type: "error", JobID: jobID, Message: "Операция отменена"})
}

func isPDFJobCanceled(jobID string) bool {
	pdfJobsLock.Lock()
	defer pdfJobsLock.Unlock()
	job := pdfJobs[jobID]
	return job == nil || job.Canceled
}

func watchPDFJob(conn io.Writer, jobID, ownerToken string) {
	if jobID == "" || ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "jobId и ownerToken обязательны"})
		return
	}

	lastSignature := ""
	for {
		message, signature, terminal, ok := pdfJobSnapshot(jobID, ownerToken)
		if !ok {
			_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "задача не найдена или принадлежит другому пользователю"})
			return
		}
		if signature != lastSignature {
			if err := writeServerJSON(conn, message); err != nil {
				return
			}
			lastSignature = signature
		}
		if terminal {
			return
		}
		time.Sleep(time.Second)
	}
}

func pdfJobSnapshot(jobID, ownerToken string) (wsMessage, string, bool, bool) {
	pdfJobsLock.Lock()
	defer pdfJobsLock.Unlock()

	job := pdfJobs[jobID]
	if job == nil || job.OwnerToken != ownerToken {
		return wsMessage{}, "", false, false
	}

	switch job.Status {
	case "done":
		message := wsMessage{
			Type:       "result",
			JobID:      job.ID,
			Current:    job.Current,
			Total:      job.Total,
			FileName:   job.FileName,
			FileBase64: job.FileBase64,
			Processed:  job.Processed,
			Failed:     job.Failed,
			Message:    job.Message,
			Logs:       append([]string(nil), job.Logs...),
		}
		return message, fmt.Sprintf("done:%d:%d:%d:%s", job.Processed, job.Failed, len(job.Logs), job.FileName), true, true
	case "paused":
		message := wsMessage{
			Type:      "auth",
			JobID:     job.ID,
			Current:   job.Current,
			Total:     job.Total,
			Number:    job.Number,
			Message:   job.Message,
			Processed: job.Processed,
			Failed:    job.Failed,
			Logs:      append([]string(nil), job.Logs...),
		}
		return message, fmt.Sprintf("paused:%d:%d:%s", job.Current, len(job.Logs), job.Message), false, true
	case "error":
		message := wsMessage{
			Type:    "error",
			JobID:   job.ID,
			Current: job.Current,
			Total:   job.Total,
			Message: job.Message,
			Error:   job.Error,
			Failed:  job.Failed,
			Logs:    append([]string(nil), job.Logs...),
		}
		return message, fmt.Sprintf("error:%d:%d:%s", job.Current, len(job.Logs), job.Error), true, true
	default:
		message := wsMessage{
			Type:      "progress",
			JobID:     job.ID,
			Current:   job.Current,
			Total:     job.Total,
			Number:    job.Number,
			Status:    job.Message,
			Error:     job.Error,
			Processed: job.Processed,
			Failed:    job.Failed,
			Logs:      append([]string(nil), job.Logs...),
		}
		return message, fmt.Sprintf("running:%d:%d:%d:%d:%s:%s", job.Current, job.Processed, job.Failed, len(job.Logs), job.Number, job.Error), false, true
	}
}

func newJobID() string {
	var bytesValue [16]byte
	if _, err := rand.Read(bytesValue[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return fmt.Sprintf("%x", bytesValue[:])
}

func newHTTPStatusError(statusCode int, body []byte) error {
	return httpStatusError{
		StatusCode: statusCode,
		Body:       strings.TrimSpace(string(body)),
	}
}

func isAuthExpiredError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr httpStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusUnauthorized
}

func handleExecProcWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	payload, err := readClientTextFrame(conn)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	var req execExportRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось разобрать запрос выгрузки"})
		return
	}

	if req.Type == "execproc-watch" {
		watchPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "execproc-cancel" {
		cancelPDFJob(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken))
		return
	}
	if req.Type == "execproc-token" {
		updatePDFJobSession(conn, strings.TrimSpace(req.JobID), strings.TrimSpace(req.OwnerToken), strings.TrimSpace(req.SessionKey))
		return
	}

	ownerToken := strings.TrimSpace(req.OwnerToken)
	if ownerToken == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "ownerToken обязателен"})
		return
	}

	if strings.TrimSpace(req.SessionKey) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "SESSION обязателен"})
		return
	}

	startDate := strings.TrimSpace(req.StartDate)
	if startDate == "" {
		startDate = defaultExecProcStartDate
	}
	if _, err := time.Parse("2006-01-02", startDate); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "START_DATE должен быть в формате YYYY-MM-DD"})
		return
	}

	statuses := normalizeStatuses(req.Statuses)
	if len(statuses) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "выберите хотя бы один статус"})
		return
	}

	errorLogFile := ""
	jobID := newJobID()
	downloadDir := strings.TrimSpace(req.DownloadDir)
	if downloadDir == "" {
		downloadDir = filepath.Join("downloads", jobID)
	}
	outputFile := strings.TrimSpace(req.OutputFile)
	if outputFile == "" {
		outputFile = filepath.Join(downloadDir, datedExecProcResultFileName(statuses))
	} else if filepath.Base(outputFile) == outputFile {
		outputFile = filepath.Join(downloadDir, outputFile)
	}
	createPDFJob(jobID, ownerToken, 100, "Подготовка", req.SessionKey)
	_ = writeServerJSON(conn, wsMessage{Type: "job", JobID: jobID, Total: 100, Message: "Задача запущена"})

	sendLog := func(message string) error {
		if isPDFJobCanceled(jobID) {
			return errors.New("операция отменена")
		}
		log.Print(message)
		if errorLogFile != "" && isProblemLog(message) {
			if err := appendLine(errorLogFile, time.Now().Format(time.RFC3339)+" "+message); err != nil {
				log.Printf("не удалось сохранить лог ошибки: %v", err)
			}
		}
		appendPDFJobLog(jobID, message)
		_ = writeServerJSON(conn, wsMessage{Type: "log", JobID: jobID, Message: message})
		return nil
	}
	sendProgress := func(current, total int, status string) error {
		if isPDFJobCanceled(jobID) {
			return errors.New("операция отменена")
		}
		if current < 0 {
			current = 0
		}
		if current > total {
			current = total
		}
		updatePDFJob(jobID, func(job *pdfJobState) {
			job.Current = current
			job.Total = total
			job.Message = status
		})
		_ = writeServerJSON(conn, wsMessage{
			Type:    "progress",
			JobID:   jobID,
			Current: current,
			Total:   total,
			Status:  status,
		})
		return nil
	}

	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		finishPDFJobError(jobID, fmt.Sprintf("не удалось создать папку %s: %v", downloadDir, err))
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: fmt.Sprintf("не удалось создать папку %s: %v", downloadDir, err)})
		return
	}
	errorLogFile = filepath.Join(downloadDir, "errors_"+time.Now().Format("20060102_150405")+".log")
	if err := sendLog("[INFO] Error log file: " + errorLogFile); err != nil {
		return
	}
	if err := sendProgress(1, 100, "Подготовка"); err != nil {
		return
	}

	var files []string
	var stats *exportStats
	for {
		session, waitErr := waitForActiveJobSession(jobID)
		if waitErr != nil {
			return
		}
		files, stats, err = runExecProcExport(session, startDate, statuses, downloadDir, sendLog, sendProgress)
		if err == nil {
			break
		}
		if isAuthExpiredError(err) {
			pausePDFJobForAuth(jobID)
			_ = writeServerJSON(conn, wsMessage{
				Type:    "auth",
				JobID:   jobID,
				Current: 0,
				Total:   100,
				Message: "SESSION больше не валиден. Обновите токен, чтобы продолжить.",
			})
			if _, waitErr := waitForActiveJobSession(jobID); waitErr != nil {
				return
			}
			continue
		}
		finishPDFJobError(jobID, err.Error())
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if len(files) == 0 {
		finishPDFJobError(jobID, "по выбранным статусам данные не найдены")
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "по выбранным статусам данные не найдены"})
		return
	}

	if err := sendLog("[INFO] Merging Excel files..."); err != nil {
		return
	}
	if err := sendProgress(92, 100, "Объединение Excel"); err != nil {
		return
	}
	rows, removed, err := mergeExecProcFiles(files)
	if err != nil {
		finishPDFJobError(jobID, err.Error())
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Removing duplicates... removed: %d", removed)); err != nil {
		return
	}
	finalRows := len(rows) - 1
	if err := sendLog(fmt.Sprintf("[INFO] Full page totalElements: %d", stats.FullTotal)); err != nil {
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Leaf range totalElements: %d", stats.LeafTotal)); err != nil {
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Exported rows before merge dedupe: %d", stats.ExportedRows)); err != nil {
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Final rows after dedupe: %d", finalRows)); err != nil {
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Skipped ranges: %d", stats.SkippedRanges)); err != nil {
		return
	}
	if err := sendProgress(96, 100, "Сохранение результата"); err != nil {
		return
	}

	xlsxBytes, err := buildXLSXBytes([]sheetData{{Name: "Результаты", Rows: rows}})
	if err != nil {
		finishPDFJobError(jobID, err.Error())
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if err := os.WriteFile(outputFile, xlsxBytes, 0644); err != nil {
		finishPDFJobError(jobID, fmt.Sprintf("не удалось сохранить %s: %v", outputFile, err))
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: fmt.Sprintf("не удалось сохранить %s: %v", outputFile, err)})
		return
	}
	if err := sendLog("[INFO] Saved " + outputFile); err != nil {
		return
	}
	if err := sendProgress(98, 100, "Очистка промежуточных файлов"); err != nil {
		return
	}
	removedFiles, cleanupErr := cleanupDownloadedFiles(files)
	if cleanupErr != nil {
		if err := sendLog(fmt.Sprintf("[WARN] Cleanup failed after removing %d files: %v", removedFiles, cleanupErr)); err != nil {
			return
		}
	} else if err := sendLog(fmt.Sprintf("[INFO] Removed intermediate files: %d", removedFiles)); err != nil {
		return
	}
	if err := sendProgress(100, 100, "Готово"); err != nil {
		return
	}
	if isPDFJobCanceled(jobID) {
		return
	}

	_ = writeServerJSON(conn, wsMessage{
		Type:       "result",
		JobID:      jobID,
		FileName:   filepath.Base(outputFile),
		FileBase64: base64.StdEncoding.EncodeToString(xlsxBytes),
		Processed:  finalRows,
		Failed:     removed,
	})
	finishPDFJobResult(jobID, filepath.Base(outputFile), base64.StdEncoding.EncodeToString(xlsxBytes), finalRows, removed)
}

type exportStats struct {
	FullTotal     int
	LeafTotal     int
	ExportedRows  int
	SkippedRanges int
	mu            sync.Mutex
}

func (s *exportStats) addFullTotal(value int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.FullTotal += value
}

func (s *exportStats) addLeafTotal(value int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LeafTotal += value
}

func (s *exportStats) addExportedRows(value int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ExportedRows += value
}

func (s *exportStats) addSkippedRange() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SkippedRanges++
}

func runExecProcExport(session, startDate string, statuses []string, downloadDir string, sendLog func(string) error, sendProgress func(int, int, string) error) ([]string, *exportStats, error) {
	stats := &exportStats{}
	files := make([]string, 0)
	endDate := time.Now().Format("2006-01-02")

	const compactLimit = 9500

	for statusIndex, statusCode := range statuses {
		statusLabel := statusCode
		if statusLabel == "" {
			statusLabel = "all"
		}
		statusStart := 2 + (88*statusIndex)/len(statuses)
		statusPlanDone := statusStart + 22/len(statuses)
		statusEnd := 2 + (88*(statusIndex+1))/len(statuses)

		if err := sendProgress(statusStart, 100, "Планирование "+statusLabel); err != nil {
			return files, stats, err
		}

		fullRange := dateRange{
			From:  startDate,
			To:    endDate,
			Label: strings.ReplaceAll(startDate, "-", "_") + "_" + strings.ReplaceAll(endDate, "-", "_"),
		}

		fullSearchResult, err := searchDateRange(session, statusCode, fullRange, sendLog)
		if err != nil {
			return files, stats, err
		}

		stats.addFullTotal(fullSearchResult.TotalElements)

		if err := sendLog(fmt.Sprintf("[INFO] Status %s full page totalElements: %d", statusLabel, fullSearchResult.TotalElements)); err != nil {
			return files, stats, err
		}

		plannedRanges, err := planExportRangesWithSearch(session, statusCode, statusLabel, fullRange, fullSearchResult, sendLog)
		if err != nil {
			return files, stats, err
		}

		if err := sendLog(fmt.Sprintf("[INFO] Planned ranges for status %s before compact: %d", statusLabel, len(plannedRanges))); err != nil {
			return files, stats, err
		}

		plannedRanges, err = compactPlannedRanges(session, statusCode, statusLabel, plannedRanges, compactLimit, sendLog)
		if err != nil {
			return files, stats, err
		}

		if err := sendLog(fmt.Sprintf("[INFO] Planned ranges for status %s after compact: %d", statusLabel, len(plannedRanges))); err != nil {
			return files, stats, err
		}

		if err := sendProgress(statusPlanDone, 100, "Выгрузка "+statusLabel); err != nil {
			return files, stats, err
		}

		downloadedFiles, err := downloadPlannedRanges(session, statusCode, statusLabel, downloadDir, plannedRanges, sendLog, sendProgress, statusPlanDone, statusEnd, stats)
		if err != nil {
			return files, stats, err
		}

		files = append(files, downloadedFiles...)

		if err := sendProgress(statusEnd, 100, "Status "+statusLabel+" готов"); err != nil {
			return files, stats, err
		}
	}

	return files, stats, nil
}

func cleanupDownloadedFiles(files []string) (int, error) {
	removed := 0
	for _, fileName := range files {
		if strings.TrimSpace(fileName) == "" {
			continue
		}
		if err := os.Remove(fileName); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return removed, fmt.Errorf("не удалось удалить %s: %w", fileName, err)
		}
		removed++
	}
	return removed, nil
}

func compactPlannedRanges(
	session, statusCode, statusLabel string,
	ranges []plannedRange,
	limit int,
	sendLog func(string) error,
) ([]plannedRange, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	compacted := make([]plannedRange, 0, len(ranges))

	currentFrom := ranges[0].Range.From
	currentTo := ranges[0].Range.To
	currentSearch := ranges[0].Search

	for i := 1; i < len(ranges); i++ {
		nextRange := ranges[i].Range

		candidateRange := dateRange{
			From:  currentFrom,
			To:    nextRange.To,
			Label: strings.ReplaceAll(currentFrom, "-", "_") + "_" + strings.ReplaceAll(nextRange.To, "-", "_"),
		}

		candidateSearch, err := searchDateRange(session, statusCode, candidateRange, sendLog)
		if err != nil {
			return nil, wrapRangeError(statusLabel, candidateRange, err)
		}

		if candidateSearch.TotalElements <= limit {
			currentTo = nextRange.To
			currentSearch = candidateSearch
			continue
		}

		finalRange := dateRange{
			From:  currentFrom,
			To:    currentTo,
			Label: strings.ReplaceAll(currentFrom, "-", "_") + "_" + strings.ReplaceAll(currentTo, "-", "_"),
		}

		compacted = append(compacted, plannedRange{
			Range:  finalRange,
			Search: currentSearch,
		})

		currentFrom = nextRange.From
		currentTo = nextRange.To
		currentSearch = ranges[i].Search
	}

	finalRange := dateRange{
		From:  currentFrom,
		To:    currentTo,
		Label: strings.ReplaceAll(currentFrom, "-", "_") + "_" + strings.ReplaceAll(currentTo, "-", "_"),
	}

	compacted = append(compacted, plannedRange{
		Range:  finalRange,
		Search: currentSearch,
	})

	if err := sendLog(fmt.Sprintf("[INFO] Compacted ranges for status %s: %d -> %d", statusLabel, len(ranges), len(compacted))); err != nil {
		return nil, err
	}

	return compacted, nil
}
func planExportRanges(session, statusCode, statusLabel string, currentRange dateRange, sendLog func(string) error) ([]plannedRange, error) {
	searchResult, err := searchDateRange(session, statusCode, currentRange, sendLog)
	if err != nil {
		return nil, wrapRangeError(statusLabel, currentRange, err)
	}
	return planExportRangesWithSearch(session, statusCode, statusLabel, currentRange, searchResult, sendLog)
}

func planExportRangesWithSearch(session, statusCode, statusLabel string, currentRange dateRange, searchResult execProcSearchResult, sendLog func(string) error) ([]plannedRange, error) {
	if searchResult.TotalElements == 0 {
		return nil, nil
	}
	if searchResult.TotalElements <= 10000 {
		return []plannedRange{{Range: currentRange, Search: searchResult}}, nil
	}

	left, right, err := splitDateRange(currentRange)
	if err != nil {
		return nil, wrapRangeError(statusLabel, currentRange, err)
	}
	if err := sendLog(fmt.Sprintf("[INFO] Split range %s (%s - %s), totalElements=%d", currentRange.Label, currentRange.From, currentRange.To, searchResult.TotalElements)); err != nil {
		return nil, err
	}

	result := make([]plannedRange, 0)
	leftRanges, err := planExportRanges(session, statusCode, statusLabel, left, sendLog)
	if err != nil {
		return result, err
	}
	result = append(result, leftRanges...)
	rightRanges, err := planExportRanges(session, statusCode, statusLabel, right, sendLog)
	if err != nil {
		return result, err
	}
	result = append(result, rightRanges...)
	return result, nil
}

func downloadPlannedRanges(session, statusCode, statusLabel, downloadDir string, ranges []plannedRange, sendLog func(string) error, sendProgress func(int, int, string) error, progressStart, progressEnd int, stats *exportStats) ([]string, error) {
	files := make([]string, 0, len(ranges))
	for idx, current := range ranges {
		progress := progressStart
		if len(ranges) > 0 {
			progress = progressStart + ((progressEnd - progressStart) * (idx + 1) / len(ranges))
		}
		if err := sendProgress(progress, 100, "Скачивание "+current.Range.Label); err != nil {
			return files, err
		}
		downloaded, err := exportPlannedRange(session, statusCode, statusLabel, downloadDir, current, sendLog, stats)
		if err != nil {
			files = append(files, downloaded...)
			wrapped := wrapRangeError(statusLabel, current.Range, err)
			if logErr := sendLog("[ERROR] Skipped " + wrapped.Error()); logErr != nil {
				return files, logErr
			}
			stats.addSkippedRange()
			continue
		}
		files = append(files, downloaded...)
	}
	return files, nil
}

func exportPlannedRange(session, statusCode, statusLabel, downloadDir string, current plannedRange, sendLog func(string) error, stats *exportStats) ([]string, error) {
	freshSearch, err := searchDateRange(session, statusCode, current.Range, sendLog)
	if err != nil {
		return nil, err
	}
	if freshSearch.TotalElements == 0 {
		if err := sendLog("[INFO] Range " + current.Range.Label + " is empty at download time"); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if freshSearch.TotalElements > 10000 {
		if err := sendLog(fmt.Sprintf("[INFO] Re-splitting range %s before download, totalElements=%d", current.Range.Label, freshSearch.TotalElements)); err != nil {
			return nil, err
		}
		subRanges, err := planExportRangesWithSearch(session, statusCode, statusLabel, current.Range, freshSearch, sendLog)
		if err != nil {
			return nil, err
		}
		files := make([]string, 0, len(subRanges))
		for _, subRange := range subRanges {
			downloaded, err := exportPlannedRange(session, statusCode, statusLabel, downloadDir, subRange, sendLog, stats)
			if err != nil {
				return files, err
			}
			files = append(files, downloaded...)
		}
		return files, nil
	}
	current.Search = freshSearch
	if strings.TrimSpace(current.Search.SearchID) == "" {
		return nil, fmt.Errorf("searchId пустой при totalElements=%d", current.Search.TotalElements)
	}

	fileName := filepath.Join(downloadDir, fmt.Sprintf("status_%s_%s.xlsx", statusLabel, current.Range.Label))
	if err := downloadExecProcExcel(session, current.Search.SearchID, statusCode, fileName); err != nil {
		return nil, err
	}
	if err := sendLog("[INFO] Downloaded: " + fileName); err != nil {
		return nil, err
	}

	exportedRows, err := dataRowCountFromFile(fileName)
	if err != nil {
		return nil, err
	}
	if err := sendLog(fmt.Sprintf("[INFO] Exported rows: %d", exportedRows)); err != nil {
		return nil, err
	}
	stats.addLeafTotal(current.Search.TotalElements)
	stats.addExportedRows(exportedRows)
	if exportedRows > current.Search.TotalElements {
		if err := sendLog(fmt.Sprintf("[WARN] Range %s exported more rows than search totalElements: totalElements=%d exportedRows=%d", current.Range.Label, current.Search.TotalElements, exportedRows)); err != nil {
			return nil, err
		}
	}
	if current.Search.TotalElements > exportedRows && exportedRows >= 10000 {
		if err := sendLog(fmt.Sprintf("[WARN] Range %s may be truncated: totalElements=%d exportedRows=%d", current.Range.Label, current.Search.TotalElements, exportedRows)); err != nil {
			return nil, err
		}
	}

	return []string{fileName}, nil
}

func wrapRangeError(statusLabel string, currentRange dateRange, err error) error {
	return fmt.Errorf("status %s range %s (%s - %s): %w", statusLabel, currentRange.Label, currentRange.From, currentRange.To, err)
}

func searchDateRange(session, statusCode string, currentRange dateRange, sendLog func(string) error) (execProcSearchResult, error) {
	searchResult, err := searchExecProc(session, currentRange.From, currentRange.To, statusCode, sendLog)
	if err != nil {
		return execProcSearchResult{}, err
	}
	if err := sendLog(fmt.Sprintf("[INFO] Found totalElements: %d", searchResult.TotalElements)); err != nil {
		return execProcSearchResult{}, err
	}
	if err := sendLog("[INFO] searchId: " + searchResult.SearchID); err != nil {
		return execProcSearchResult{}, err
	}
	return searchResult, nil
}

type dateRange struct {
	From  string
	To    string
	Label string
}

type plannedRange struct {
	Range  dateRange
	Search execProcSearchResult
}

func splitDateRange(currentRange dateRange) (dateRange, dateRange, error) {
	start, err := time.Parse("2006-01-02", currentRange.From)
	if err != nil {
		return dateRange{}, dateRange{}, err
	}
	end, err := time.Parse("2006-01-02", currentRange.To)
	if err != nil {
		return dateRange{}, dateRange{}, err
	}
	if !start.Before(end) {
		return dateRange{}, dateRange{}, fmt.Errorf("диапазон %s нельзя разделить дальше", currentRange.Label)
	}

	days := int(end.Sub(start).Hours()/24) + 1
	leftDays := days / 2
	if leftDays < 1 {
		leftDays = 1
	}
	leftEnd := start.AddDate(0, 0, leftDays-1)
	rightStart := leftEnd.AddDate(0, 0, 1)
	return newDateRange(start, leftEnd), newDateRange(rightStart, end), nil
}

func newDateRange(start, end time.Time) dateRange {
	from := start.Format("2006-01-02")
	to := end.Format("2006-01-02")
	label := strings.ReplaceAll(from, "-", "_")
	if from != to {
		label += "_" + strings.ReplaceAll(to, "-", "_")
	}
	return dateRange{
		From:  from,
		To:    to,
		Label: label,
	}
}

type execProcSearchResult struct {
	TotalElements int
	SearchID      string
}

func searchExecProc(session, fromDate, toDate, statusCode string, sendLog func(string) error) (execProcSearchResult, error) {
	payload := map[string]any{
		"searchType": false,
	}
	if strings.TrimSpace(fromDate) != "" {
		payload["fromDate"] = fromDate
	}
	if strings.TrimSpace(toDate) != "" {
		payload["toDate"] = toDate
	}
	if strings.TrimSpace(statusCode) != "" {
		payload["statusCode"] = statusCode
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return execProcSearchResult{}, err
	}
	if sendLog != nil {
		if err := sendLog("[DEBUG] Search curl: " + buildSearchCurl(session, string(body))); err != nil {
			return execProcSearchResult{}, err
		}
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/rest/execproc/search?page=0&size=5", bytes.NewReader(body))
	if err != nil {
		return execProcSearchResult{}, err
	}
	addExecProcHeaders(req, session)
	req.Header.Set("Content-Type", "application/json")

	client, err := getHTTPClient()
	if err != nil {
		return execProcSearchResult{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return execProcSearchResult{}, fmt.Errorf("ошибка поиска status=%s from=%s: %w", statusCode, fromDate, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return execProcSearchResult{}, fmt.Errorf("не удалось прочитать ответ поиска: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return execProcSearchResult{}, fmt.Errorf("поиск вернул ошибку: %w", newHTTPStatusError(resp.StatusCode, respBody))
	}

	var parsed struct {
		Pagination struct {
			TotalElements int    `json:"totalElements"`
			SearchID      string `json:"searchId"`
		} `json:"pagination"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return execProcSearchResult{}, fmt.Errorf("не удалось разобрать ответ поиска: %w", err)
	}

	return execProcSearchResult{
		TotalElements: parsed.Pagination.TotalElements,
		SearchID:      parsed.Pagination.SearchID,
	}, nil
}

func buildSearchCurl(session, body string) string {
	return "curl 'https://aisoip.adilet.gov.kz/extperson/api/rest/execproc/search?page=0&size=5' " +
		"-H 'Accept: application/json, text/plain, */*' " +
		"-H 'Accept-Language: ru' " +
		"-H 'Connection: keep-alive' " +
		"-H 'Content-Type: application/json' " +
		"-b '" + maskCookieHeader(cookieHeaderValue(session)) + "' " +
		"-H 'Origin: https://aisoip.adilet.gov.kz' " +
		"-H 'Referer: https://aisoip.adilet.gov.kz/cabinet/exec-productions' " +
		"-H 'Sec-Fetch-Dest: empty' " +
		"-H 'Sec-Fetch-Mode: cors' " +
		"-H 'Sec-Fetch-Site: same-origin' " +
		"-H 'User-Agent: " + execProcUA + "' " +
		"-H 'sec-ch-ua: \"Chromium\";v=\"148\", \"Microsoft Edge\";v=\"148\", \"Not/A)Brand\";v=\"99\"' " +
		"-H 'sec-ch-ua-mobile: ?1' " +
		"-H 'sec-ch-ua-platform: \"Android\"' " +
		"--data-raw '" + strings.ReplaceAll(body, "'", "'\\''") + "'"
}

func cookieHeaderValue(session string) string {
	session = strings.TrimSpace(session)
	if extracted := extractCookieFromCurl(session); extracted != "" {
		session = extracted
	}
	if strings.Contains(session, "=") {
		return session
	}
	return "SESSION=" + session
}

func extractCookieFromCurl(value string) string {
	value = strings.TrimSpace(value)
	if !strings.Contains(value, "curl ") && !strings.Contains(value, " -b ") && !strings.Contains(value, "--cookie") {
		return ""
	}

	for _, flag := range []string{"-b", "--cookie"} {
		idx := strings.Index(value, flag)
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(value[idx+len(flag):])
		if rest == "" {
			continue
		}
		if rest[0] == '\'' || rest[0] == '"' {
			quote := rest[0]
			rest = rest[1:]
			end := strings.IndexByte(rest, quote)
			if end >= 0 {
				return strings.TrimSpace(rest[:end])
			}
			return strings.TrimSpace(rest)
		}
		end := strings.IndexByte(rest, ' ')
		if end >= 0 {
			return strings.TrimSpace(rest[:end])
		}
		return strings.TrimSpace(rest)
	}

	return ""
}

func maskCookieHeader(value string) string {
	parts := strings.Split(value, ";")
	for idx, part := range parts {
		part = strings.TrimSpace(part)
		name, secret, ok := strings.Cut(part, "=")
		if !ok {
			parts[idx] = maskSecret(part)
			continue
		}
		parts[idx] = strings.TrimSpace(name) + "=" + maskSecret(secret)
	}
	return strings.Join(parts, "; ")
}

func maskSecret(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 12 {
		return "***"
	}
	return value[:6] + "..." + value[len(value)-6:]
}

func downloadExecProcExcel(session, searchID, statusCode, fileName string) error {
	u, err := url.Parse(baseURL + "/api/rest/export/excel")
	if err != nil {
		return err
	}
	query := u.Query()
	query.Set("searchtype", "false")
	query.Set("page", "0")
	query.Set("size", "50000")
	query.Set("searchid", searchID)
	query.Set("lang", "ru")
	u.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	addExecProcHeaders(req, session)

	client, err := getHTTPClient()
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка скачивания Excel searchId=%s: %w", searchID, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("не удалось прочитать Excel: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Excel endpoint вернул ошибку: %w", newHTTPStatusError(resp.StatusCode, data))
	}
	if len(data) < 4 || string(data[:2]) != "PK" {
		return fmt.Errorf("Excel не скачался: ответ не похож на .xlsx, размер %d байт", len(data))
	}

	if err := os.WriteFile(fileName, data, 0644); err != nil {
		return fmt.Errorf("не удалось сохранить %s: %w", fileName, err)
	}
	return nil
}

func addExecProcHeaders(req *http.Request, session string) {
	req.Header.Set("Cookie", cookieHeaderValue(session))
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "ru")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Origin", "https://aisoip.adilet.gov.kz")
	req.Header.Set("Referer", "https://aisoip.adilet.gov.kz/cabinet/exec-productions")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("User-Agent", execProcUA)
	req.Header.Set("sec-ch-ua", `"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"`)
	req.Header.Set("sec-ch-ua-mobile", "?1")
	req.Header.Set("sec-ch-ua-platform", `"Android"`)
}

func normalizeStatuses(statuses []string) []string {
	allowed := map[string]struct{}{"1": {}, "2": {}, "3": {}, "50": {}, "51": {}, "52": {}}
	if len(statuses) == 0 {
		return []string{""}
	}
	seen := make(map[string]struct{})
	result := make([]string, 0, len(statuses))
	for _, status := range statuses {
		status = strings.TrimSpace(status)
		if status == "" {
			return []string{""}
		}
		if _, ok := allowed[status]; !ok {
			continue
		}
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		result = append(result, status)
	}
	return result
}

func maxExcitationDateFromFile(fileName string) (string, error) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	rows, err := readXLSXRowsBytes(data)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", nil
	}
	dateIndex := findHeaderIndex(rows[0], "Дата возбуждения")
	if dateIndex < 0 {
		return "", errors.New("в Excel нет колонки «Дата возбуждения»")
	}
	maxDate := ""
	for i := 1; i < len(rows); i++ {
		if dateIndex >= len(rows[i]) {
			continue
		}
		value := normalizeExcelDate(rows[i][dateIndex])
		if value > maxDate {
			maxDate = value
		}
	}
	return maxDate, nil
}

func dataRowCountFromFile(fileName string) (int, error) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return 0, err
	}
	rows, err := readXLSXRowsBytes(data)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, row := range rows[1:] {
		if rowHasAnyValue(row) {
			count++
		}
	}
	return count, nil
}

func rowHasAnyValue(row []string) bool {
	for _, value := range row {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func mergeExecProcFiles(files []string) ([][]string, int, error) {
	files = append([]string(nil), files...)
	sort.Strings(files)

	var header []string
	uniqueRows := make([][]string, 0)
	seen := make(map[string]string)
	removed := 0

	for _, fileName := range files {
		data, err := os.ReadFile(fileName)
		if err != nil {
			return nil, removed, err
		}
		rows, err := readXLSXRowsBytes(data)
		if err != nil {
			return nil, removed, fmt.Errorf("%s: %w", fileName, err)
		}
		if len(rows) == 0 {
			continue
		}
		if header == nil {
			header = rows[0]
		}
		keyIndexes := duplicateKeyIndexes(rows[0])
		for _, row := range rows[1:] {
			key := duplicateKey(row, keyIndexes)
			if strings.TrimSpace(key) == "" {
				key = duplicateFallbackKey(row)
			}
			if firstFile, ok := seen[key]; ok {
				if firstFile != fileName {
					removed++
					continue
				}
			} else {
				seen[key] = fileName
			}
			uniqueRows = append(uniqueRows, row)
		}
	}

	if header == nil {
		return nil, removed, errors.New("нет строк для объединения")
	}
	return append([][]string{header}, uniqueRows...), removed, nil
}

func duplicateKeyIndexes(header []string) []int {
	if idx := findHeaderIndex(header, "execProcId"); idx >= 0 {
		return []int{idx}
	}
	return nil
}

func duplicateFallbackKey(row []string) string {
	if len(row) <= 1 {
		return strings.Join(row, "\x1f")
	}
	return strings.Join(row[1:], "\x1f")
}

func duplicateKey(row []string, indexes []int) string {
	parts := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		if idx >= len(row) {
			parts = append(parts, "")
			continue
		}
		parts = append(parts, strings.TrimSpace(row[idx]))
	}
	return strings.Join(parts, "\x1f")
}

func findHeaderIndex(header []string, name string) int {
	target := strings.ToLower(strings.TrimSpace(name))
	for idx, value := range header {
		if strings.ToLower(strings.TrimSpace(value)) == target {
			return idx
		}
	}
	return -1
}

func findFirstHeaderIndex(header []string, names ...string) int {
	for _, name := range names {
		if idx := findHeaderIndex(header, name); idx >= 0 {
			return idx
		}
	}
	return -1
}

func readNumbersFromXLSXBytes(data []byte) ([]string, error) {
	rows, err := readXLSXRowsBytes(data)
	if err == nil {
		numbers := make([]string, 0, len(rows))
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			value := strings.TrimSpace(row[0])
			if value == "" {
				continue
			}
			lowerValue := strings.ToLower(value)
			if lowerValue == "number" || strings.Contains(lowerValue, "номер") {
				continue
			}
			numbers = append(numbers, value)
		}
		return numbers, nil
	}

	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("не удалось открыть xlsx: %w", err)
	}

	sharedStrings, _ := readSharedStrings(reader)
	sheetXML, err := readZipFile(reader, "xl/worksheets/sheet1.xml")
	if err != nil {
		return nil, err
	}

	var sheet xlsxWorksheet
	if err := xml.Unmarshal(sheetXML, &sheet); err != nil {
		return nil, fmt.Errorf("не удалось прочитать sheet1.xml: %w", err)
	}

	numbers := make([]string, 0, len(sheet.SheetData.Rows))
	for _, row := range sheet.SheetData.Rows {
		if len(row.Cells) == 0 {
			continue
		}
		value := strings.TrimSpace(resolveCellValue(row.Cells[0], sharedStrings))
		if value == "" {
			continue
		}
		lowerValue := strings.ToLower(value)
		if lowerValue == "number" || strings.Contains(lowerValue, "номер") {
			continue
		}
		numbers = append(numbers, value)
	}

	return numbers, nil
}

func readIINsFromXLSXBytes(data []byte) ([]string, error) {
	rows, err := readXLSXRowsBytes(data)
	if err != nil {
		return nil, err
	}

	iins := make([]string, 0, len(rows))
	seen := make(map[string]bool)
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		iin := strings.TrimSpace(row[0])
		if iin == "" {
			continue
		}
		lower := strings.ToLower(iin)
		if lower == "iin" || lower == "иин" || strings.Contains(lower, "иин") {
			continue
		}
		iin = onlyDigits(iin)
		if iin == "" || seen[iin] {
			continue
		}
		seen[iin] = true
		iins = append(iins, iin)
	}
	return iins, nil
}

func onlyDigits(value string) string {
	var builder strings.Builder
	for _, r := range value {
		if r >= '0' && r <= '9' {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func readTargetStatusNumbersFromXLSXBytes(data []byte) ([]string, error) {
	rows, err := readXLSXRowsBytes(data)
	if err != nil {
		return nil, err
	}

	numbers := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		number := strings.TrimSpace(row[0])
		if number == "" {
			continue
		}
		lowerNumber := strings.ToLower(number)
		if lowerNumber == "number" || strings.Contains(lowerNumber, "номер") {
			continue
		}

		status := ""
		if len(row) > 1 {
			status = strings.TrimSpace(row[1])
		}
		if !strings.EqualFold(status, targetExecProcStatus) {
			continue
		}
		numbers = append(numbers, number)
	}

	return numbers, nil
}

func pdfParseJobsFromRows(rows [][]string) []pdfParseJob {
	jobs := make([]pdfParseJob, 0, len(rows))
	for idx, row := range rows {
		if len(row) == 0 {
			continue
		}
		number := strings.TrimSpace(row[0])
		if number == "" {
			continue
		}
		lowerNumber := strings.ToLower(number)
		if lowerNumber == "number" || strings.Contains(lowerNumber, "номер") {
			continue
		}

		jobs = append(jobs, pdfParseJob{
			Index:    len(jobs),
			RowIndex: idx,
			Number:   number,
		})
	}
	return jobs
}

func strictPDFParseJobsFromRows(rows [][]string) ([]pdfParseJob, error) {
	jobs := make([]pdfParseJob, 0, len(rows))
	for idx, row := range rows {
		if rowIsEmpty(row) {
			continue
		}
		if idx == 0 && isStrictPDFHeaderRow(row) {
			continue
		}
		if len(row) < 2 || strings.TrimSpace(row[0]) == "" || strings.TrimSpace(row[1]) == "" {
			return nil, fmt.Errorf("строка %d: нужны ровно две заполненные колонки: исполнительный документ и исполнительное производство", idx+1)
		}
		for colIdx := 2; colIdx < len(row); colIdx++ {
			if strings.TrimSpace(row[colIdx]) != "" {
				return nil, fmt.Errorf("строка %d: файл должен содержать строго две колонки, найдена лишняя колонка %d", idx+1, colIdx+1)
			}
		}

		jobs = append(jobs, pdfParseJob{
			Index:       len(jobs),
			RowIndex:    idx,
			Number:      strings.TrimSpace(row[0]),
			ExecProcNum: strings.TrimSpace(row[1]),
		})
	}
	return jobs, nil
}

func rowIsEmpty(row []string) bool {
	for _, value := range row {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

func isStrictPDFHeaderRow(row []string) bool {
	if len(row) < 2 {
		return false
	}
	first := strings.ToLower(strings.TrimSpace(row[0]))
	second := strings.ToLower(strings.TrimSpace(row[1]))
	return (strings.Contains(first, "документ") || strings.Contains(first, "execdoc")) &&
		(strings.Contains(second, "производ") || strings.Contains(second, "execproc"))
}

func appendPDFParseColumns(rows [][]string) [][]string {
	result := make([][]string, len(rows))
	for idx, row := range rows {
		result[idx] = append([]string(nil), row...)
	}

	hasHeader := len(result) > 0 && len(result[0]) > 0 && (strings.Contains(strings.ToLower(result[0][0]), "номер") || strings.EqualFold(result[0][0], "number"))
	if hasHeader {
		result[0] = append(result[0], "Дата постановления", "Основание", "Статус обработки")
	}

	for idx := range result {
		if hasHeader && idx == 0 {
			continue
		}
		result[idx] = append(result[idx], "", "", "")
	}
	return result
}

func appendPDFUpdatedParseColumns(rows [][]string) [][]string {
	result := make([][]string, len(rows))
	for idx, row := range rows {
		result[idx] = append([]string(nil), row...)
	}

	hasHeader := len(result) > 0 && isStrictPDFHeaderRow(result[0])
	if hasHeader {
		result[0] = append(result[0], "Дата постановления", "Основание")
	}

	for idx := range result {
		if hasHeader && idx == 0 {
			continue
		}
		result[idx] = append(result[idx], "", "")
	}
	return result
}

func setPDFParseColumns(rows [][]string, rowIndex int, decreeDate, legalBasis, status string) {
	if rowIndex < 0 || rowIndex >= len(rows) {
		return
	}
	rows[rowIndex][len(rows[rowIndex])-3] = decreeDate
	rows[rowIndex][len(rows[rowIndex])-2] = legalBasis
	rows[rowIndex][len(rows[rowIndex])-1] = status
}

func setPDFUpdatedParseColumns(rows [][]string, rowIndex int, decreeDate, legalBasis string) {
	if rowIndex < 0 || rowIndex >= len(rows) {
		return
	}
	rows[rowIndex][len(rows[rowIndex])-2] = decreeDate
	rows[rowIndex][len(rows[rowIndex])-1] = legalBasis
}

func pdfParseStatus(result pdfTitleResult) string {
	if result.Err != nil {
		return "Ошибка: " + result.Err.Error()
	}
	if result.DecreeDate == "" || result.LegalBasis == "" {
		return "Частично"
	}
	return "OK"
}

func readXLSXRowsBytes(data []byte) ([][]string, error) {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("не удалось открыть xlsx: %w", err)
	}

	sharedStrings, _ := readSharedStrings(reader)
	sheetXML, err := readZipFile(reader, "xl/worksheets/sheet1.xml")
	if err != nil {
		return nil, err
	}

	var sheet xlsxWorksheet
	if err := xml.Unmarshal(sheetXML, &sheet); err != nil {
		return nil, fmt.Errorf("не удалось прочитать sheet1.xml: %w", err)
	}

	rows := make([][]string, 0, len(sheet.SheetData.Rows))
	for _, sourceRow := range sheet.SheetData.Rows {
		row := make([]string, 0, len(sourceRow.Cells))
		for fallbackIdx, cell := range sourceRow.Cells {
			colIdx := fallbackIdx
			if parsedIdx := cellColumnIndex(cell.Ref); parsedIdx >= 0 {
				colIdx = parsedIdx
			}
			for len(row) <= colIdx {
				row = append(row, "")
			}
			row[colIdx] = strings.TrimSpace(resolveCellValue(cell, sharedStrings))
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func cellColumnIndex(ref string) int {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return -1
	}
	result := 0
	seenLetter := false
	for _, r := range ref {
		switch {
		case r >= 'A' && r <= 'Z':
			result = result*26 + int(r-'A'+1)
			seenLetter = true
		case r >= 'a' && r <= 'z':
			result = result*26 + int(r-'a'+1)
			seenLetter = true
		default:
			if seenLetter {
				return result - 1
			}
		}
	}
	if !seenLetter {
		return -1
	}
	return result - 1
}

func readSharedStrings(reader *zip.Reader) ([]string, error) {
	data, err := readZipFile(reader, "xl/sharedStrings.xml")
	if err != nil {
		return nil, err
	}

	var shared xlsxSharedStrings
	if err := xml.Unmarshal(data, &shared); err != nil {
		return nil, fmt.Errorf("не удалось прочитать sharedStrings.xml: %w", err)
	}

	result := make([]string, 0, len(shared.Items))
	for _, item := range shared.Items {
		if item.Text != "" {
			result = append(result, item.Text)
			continue
		}
		var builder strings.Builder
		for _, run := range item.Runs {
			builder.WriteString(run.Text)
		}
		result = append(result, builder.String())
	}

	return result, nil
}

func readZipFile(reader *zip.Reader, name string) ([]byte, error) {
	for _, file := range reader.File {
		if file.Name != name {
			continue
		}
		rc, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("не удалось открыть %s: %w", name, err)
		}
		defer rc.Close()

		data, err := io.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("не удалось прочитать %s: %w", name, err)
		}
		return data, nil
	}

	return nil, fmt.Errorf("в xlsx не найден файл %s", name)
}

func resolveCellValue(cell xlsxCell, sharedStrings []string) string {
	switch cell.Type {
	case "inlineStr":
		return cell.InlineStr.Text
	case "s":
		index, err := strconv.Atoi(strings.TrimSpace(cell.Value))
		if err != nil || index < 0 || index >= len(sharedStrings) {
			return ""
		}
		return sharedStrings[index]
	default:
		return cell.Value
	}
}

func normalizePDFTitleWorkers(value int, total int) int {
	if value <= 0 {
		value = pdfTitleWorkerCount
	}
	if value > maxPDFTitleWorkers {
		value = maxPDFTitleWorkers
	}
	if total > 0 && value > total {
		value = total
	}
	if value < 1 {
		return 1
	}
	return value
}

func fetchDebtorERDForIIN(index int, iin string) debtorERDResult {
	result := debtorERDResult{Index: index, IIN: iin}
	searchPayload := map[string]any{
		"iin":        iin,
		"fullName":   "",
		"searchType": 0,
		"captcha":    erdCaptcha(),
		"action":     "findErd",
	}

	initialURL := debtorBaseURL + "/rest/debtor/findErd?page=0&size=10"
	var initialResponse any
	if err := debtorERDJSONRequest(http.MethodPost, initialURL, searchPayload, &initialResponse); err != nil {
		result.Err = err
		return result
	}
	searchID := extractDebtorERDSearchID(initialResponse)
	if searchID == "" {
		result.Err = errors.New("в первом ответе findErd не найден searchId")
		return result
	}

	searchURL := debtorBaseURL + "/rest/debtor/findErd?page=0&size=-1&searchId=" + url.QueryEscape(searchID)
	var searchResponse any
	if err := debtorERDJSONRequest(http.MethodPost, searchURL, searchPayload, &searchResponse); err != nil {
		result.Err = err
		return result
	}

	result.Total = extractDebtorERDTotal(searchResponse)
	items := extractDebtorERDItems(searchResponse)
	if result.Total == 0 && len(items) > 0 {
		result.Total = len(items)
	}

	rows := make([][]string, 0, len(items))
	for _, item := range items {
		detailURL, err := debtorERDDetailURL(item, searchID)
		if err != nil {
			result.Err = err
			return result
		}
		var detailResponse map[string]any
		if err := debtorERDJSONRequest(http.MethodGet, detailURL, nil, &detailResponse); err != nil {
			result.Err = err
			return result
		}
		rows = append(rows, debtorERDDetailRow(iin, result.Total, detailResponse))
	}

	result.DetailRows = rows
	result.DetailCount = len(rows)
	return result
}

func debtorERDJSONRequest(method, requestURL string, payload any, target any) error {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, requestURL, body)
	if err != nil {
		return err
	}
	setDebtorERDHeaders(req, payload != nil)

	client, err := getHTTPClient()
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return newHTTPStatusError(resp.StatusCode, data)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("не удалось разобрать JSON: %w", err)
	}
	return nil
}

func erdCaptcha() string {
	if value := strings.TrimSpace(os.Getenv("AISOIP_ERD_CAPTCHA")); value != "" {
		return value
	}
	return defaultERDCaptcha
}

func setDebtorERDHeaders(req *http.Request, hasJSONBody bool) {
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "kk")
	if hasJSONBody {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Origin", debtorBaseURL)
	req.Header.Set("Referer", debtorBaseURL+"/debtors")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("User-Agent", debtorUA)
	req.Header.Set("sec-ch-ua", `"Microsoft Edge";v="149", "Chromium";v="149", "Not)A;Brand";v="24"`)
	req.Header.Set("sec-ch-ua-mobile", "?1")
	req.Header.Set("sec-ch-ua-platform", `"Android"`)
}

func extractDebtorERDTotal(value any) int {
	for _, path := range [][]string{
		{"totalElements"},
		{"page", "totalElements"},
		{"total"},
		{"count"},
		{"result", "totalElements"},
	} {
		if found := nestedAny(value, path...); found != nil {
			if total := anyToInt(found); total >= 0 {
				return total
			}
		}
	}
	return 0
}

func extractDebtorERDSearchID(value any) string {
	for _, path := range [][]string{
		{"searchId"},
		{"searchid"},
		{"search_id"},
		{"data", "searchId"},
		{"result", "searchId"},
		{"page", "searchId"},
		{"pagination", "searchId"},
	} {
		if found := anyText(nestedAny(value, path...)); found != "" {
			return found
		}
	}

	var result string
	var walk func(any)
	walk = func(current any) {
		if result != "" {
			return
		}
		switch typed := current.(type) {
		case map[string]any:
			for key, value := range typed {
				normalized := strings.ToLower(strings.ReplaceAll(key, "_", ""))
				if normalized == "searchid" {
					if text := anyText(value); text != "" {
						result = text
						return
					}
				}
			}
			for _, value := range typed {
				walk(value)
			}
		case []any:
			for _, value := range typed {
				walk(value)
			}
		}
	}
	walk(value)
	return result
}

func extractDebtorERDItems(value any) []map[string]any {
	for _, path := range [][]string{
		{"content"},
		{"data"},
		{"items"},
		{"result"},
		{"result", "content"},
		{"rows"},
	} {
		if items := anyMapSlice(nestedAny(value, path...)); len(items) > 0 {
			return items
		}
	}
	if items := anyMapSlice(value); len(items) > 0 {
		return items
	}
	if item, ok := value.(map[string]any); ok && debtorERDItemHasDetailKeys(item) {
		return []map[string]any{item}
	}
	return nil
}

func debtorERDItemHasDetailKeys(item map[string]any) bool {
	for _, key := range []string{"id", "uuid", "typedata", "typeData", "type_data"} {
		if strings.TrimSpace(anyText(item[key])) != "" {
			return true
		}
	}
	return false
}

func debtorERDDetailURL(item map[string]any, fallbackSearchID string) (string, error) {
	id := firstAnyText(item, "id", "debtorId", "erdId")
	uuidValue := firstAnyText(item, "uid", "uuid", "UUID")
	typeData := firstAnyText(item, "typedata", "typeData", "type_data")
	searchID := firstAnyText(item, "searchid", "searchId", "search_id")
	if searchID == "" {
		searchID = fallbackSearchID
	}
	if typeData == "" {
		typeData = "0"
	}
	if id == "" || uuidValue == "" || searchID == "" {
		return "", fmt.Errorf("в ответе поиска не найден id/uuid/searchid для detail")
	}

	u, err := url.Parse(debtorBaseURL + "/rest/debtor/findErd/detail")
	if err != nil {
		return "", err
	}
	query := u.Query()
	query.Set("id", id)
	query.Set("typedata", typeData)
	query.Set("uuid", uuidValue)
	query.Set("searchid", searchID)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func debtorERDDetailRow(iin string, total int, detail map[string]any) []string {
	detailInfo := mapAny(nestedAny(detail, "detailInfo"))
	if detailInfo == nil {
		detailInfo = detail
	}
	execProc := joinNonEmpty(" ",
		firstAnyText(detailInfo, "execProcNum", "exec_proc_num"),
		firstAnyText(detailInfo, "ipStartDate", "ip_start_date"),
	)
	officer := joinNonEmpty(", ",
		firstAnyText(detailInfo, "disaDepartmentName_ru", "disaDepartmentName", "disaDepartmentAddress"),
		firstAnyText(detailInfo, "officerFullName"),
	)
	return []string{
		iin,
		strconv.Itoa(total),
		firstAnyText(detailInfo, "ilOrgan_ru", "ilOrgan", "organName_ru", "execDocIssuer", "execDocIssuer_ru"),
		execProc,
		firstAnyText(detailInfo, "recovererFullName", "recovererName"),
		firstAnyText(detailInfo, "recoveryAmount", "docDebtAmount", "debtAmount", "amount"),
		officer,
		firstAnyText(detailInfo, "status_ru", "status", "statusName_ru"),
		debtorERDObligation(detailInfo),
		"OK",
		"",
	}
}

func debtorERDObligation(detailInfo map[string]any) string {
	if value := firstAnyText(detailInfo, "nonExecContent", "notExecutedObligation", "execDocSubject", "subject_ru", "debtContent", "content_ru", "content"); value != "" {
		return value
	}
	amount := firstAnyText(detailInfo, "docDebtAmount", "recoveryAmount", "debtAmount", "amount")
	if amount == "" {
		return ""
	}
	return "Задолженность составляет " + formatDebtAmountText(amount) + " тенге"
}

func formatDebtAmountText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	parts := strings.SplitN(value, ".", 2)
	integerPart := onlyDigits(parts[0])
	if integerPart == "" {
		return value
	}

	var builder strings.Builder
	for idx, r := range integerPart {
		if idx > 0 && (len(integerPart)-idx)%3 == 0 {
			builder.WriteByte(' ')
		}
		builder.WriteRune(r)
	}
	if len(parts) == 2 {
		fraction := strings.TrimRight(onlyDigits(parts[1]), "0")
		if fraction != "" {
			builder.WriteByte(',')
			builder.WriteString(fraction)
		}
	}
	return builder.String()
}

func nestedAny(value any, path ...string) any {
	current := value
	for _, key := range path {
		item, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = item[key]
	}
	return current
}

func anyMapSlice(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if mapped, ok := item.(map[string]any); ok {
			result = append(result, mapped)
		}
	}
	return result
}

func mapAny(value any) map[string]any {
	mapped, _ := value.(map[string]any)
	return mapped
}

func firstAnyText(source map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := anyText(source[key]); value != "" {
			return value
		}
	}
	return ""
}

func anyText(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case json.Number:
		return strings.TrimSpace(typed.String())
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
	case float32:
		if typed == float32(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return ""
	}
	return text
}

func anyToInt(value any) int {
	text := anyText(value)
	if text == "" {
		return -1
	}
	if strings.Contains(text, ".") {
		floatValue, err := strconv.ParseFloat(text, 64)
		if err == nil {
			return int(floatValue)
		}
	}
	result, err := strconv.Atoi(text)
	if err != nil {
		return -1
	}
	return result
}

func joinNonEmpty(separator string, values ...string) string {
	filtered := make([]string, 0, len(values))
	seen := make(map[string]bool)
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		filtered = append(filtered, value)
	}
	return strings.Join(filtered, separator)
}

func fetchPDFTitlesForNumber(index int, rowIndex int, number, session string) pdfTitleResult {
	result := pdfTitleResult{Index: index, RowIndex: rowIndex, Number: number}

	searchPayload := map[string]any{
		"execDocNum": number,
		"searchType": false,
		"statusCode": targetExecProcStatusCode,
	}
	searchURL := baseURL + "/api/rest/execproc/search?page=0&size=5"
	var searchResponse any
	if err := execProcJSONRequest(http.MethodPost, searchURL, session, searchPayload, &searchResponse); err != nil {
		result.Err = err
		return result
	}

	execProcIDs := extractExecProcIDs(searchResponse)
	if len(execProcIDs) == 0 {
		result.Err = errors.New("в ответе поиска не найден execProcId")
		return result
	}

	return fetchPDFTitlesFromExecProcIDs(result, execProcIDs, session)
}

func fetchPDFTitlesForExactExecProc(index int, rowIndex int, number, execProcNum, session string) pdfTitleResult {
	result := pdfTitleResult{Index: index, RowIndex: rowIndex, Number: number, ExecProcNum: execProcNum}

	searchPayload := map[string]any{
		"execDocNum": number,
		"searchType": false,
		"statusCode": targetExecProcStatusCode,
	}
	searchURL := baseURL + "/api/rest/execproc/search?page=0&size=100"
	var searchResponse any
	if err := execProcJSONRequest(http.MethodPost, searchURL, session, searchPayload, &searchResponse); err != nil {
		result.Err = err
		return result
	}

	execProcID := extractExecProcIDForExecProcNum(searchResponse, execProcNum)
	if execProcID == "" {
		result.Err = fmt.Errorf("не найдено ИП с номером исполнительного производства %q", execProcNum)
		return result
	}
	result.ExecProcID = execProcID

	return fetchPDFTitlesFromExecProcIDs(result, []string{execProcID}, session)
}

func fetchPDFTitlesFromExecProcIDs(result pdfTitleResult, execProcIDs []string, session string) pdfTitleResult {
	allTitles := make([]string, 0)
	ruMatches := make([]matchedPDFDocument, 0)
	kzMatches := make([]matchedPDFDocument, 0)
	for _, execProcID := range execProcIDs {
		docURL := baseURL + "/api/rest/execproc/doc/" + url.PathEscape(execProcID) + "?searchType=false"
		var docs []map[string]any
		if err := execProcJSONRequest(http.MethodGet, docURL, session, nil, &docs); err != nil {
			result.Err = err
			return result
		}
		for _, doc := range docs {
			title := strings.TrimSpace(fmt.Sprint(doc["ddocTitle"]))
			if title == "" || title == "<nil>" {
				continue
			}
			allTitles = append(allTitles, title)
			if pdfTitleMatchesTarget(title) {
				ruMatches = append(ruMatches, matchedPDFDocument{Title: title, Doc: doc, Lang: "ru"})
				continue
			}
			if pdfTitleMatchesKazakhTarget(title) {
				kzMatches = append(kzMatches, matchedPDFDocument{Title: title, Doc: doc, Lang: "kz"})
			}
		}
	}

	result.AllTitles = append([]string(nil), allTitles...)
	result.ExecProcCount = len(execProcIDs)
	matches := ruMatches
	if len(matches) == 0 {
		matches = kzMatches
	}
	if len(matches) > 0 {
		result.Titles = matchedPDFDocumentTitles(matches)
		for _, match := range matches {
			result.PDFRefs = append(result.PDFRefs, matchedPDFDocumentRef(match))
			if result.DecreeDate != "" && result.LegalBasis != "" {
				break
			}
			pdfData, err := downloadPDFDocument(session, match.Doc, match.Lang)
			if err != nil {
				result.Err = err
				return result
			}
			if len(pdfData) == 0 {
				continue
			}
			if result.DownloadedPDF == "" {
				result.DownloadedPDF = matchedPDFDocumentRef(match)
				result.PDFSHA1 = fmt.Sprintf("%x", sha1.Sum(pdfData))
			}
			parsed, err := parsePDFWithPythonAPI(pdfData)
			if err != nil {
				result.Err = err
				return result
			}
			if result.ParserFileSHA1 == "" {
				result.ParserFileSHA1 = parsed.FileSHA1
				result.ParserTextSHA1 = parsed.TextSHA1
				result.ParserPageCount = parsed.PageCount
				result.ParserTextPreview = parsed.TextPreview
			}
			if result.DecreeDate == "" {
				result.DecreeDate = parsed.Date
			}
			if result.LegalBasis == "" {
				result.LegalBasis = parsed.Basis
			}
		}
	} else if len(allTitles) > 0 {
		result.Titles = []string{strings.Join(allTitles, "\n")}
	}
	return result
}

func fetchRequestStatusForNumber(index int, number, session string) requestStatusResult {
	result := requestStatusResult{Index: index, Number: number}

	searchPayload := map[string]any{
		"statusCode": "1",
		"execDocNum": number,
		"searchType": false,
	}
	searchURL := baseURL + "/api/rest/execproc/search?page=0&size=5"
	var searchResponse any
	if err := execProcJSONRequest(http.MethodPost, searchURL, session, searchPayload, &searchResponse); err != nil {
		result.Err = err
		return result
	}

	execProcIDs := extractExecProcIDs(searchResponse)
	result.ExecProcCount = len(execProcIDs)
	if len(execProcIDs) == 0 {
		return result
	}

	for _, execProcID := range execProcIDs {
		requestURL := baseURL + "/api/rest/execproc/request/" + url.PathEscape(execProcID) + "?searchType=false"
		var entries []requestStatusEntry
		if err := execProcJSONRequest(http.MethodGet, requestURL, session, nil, &entries); err != nil {
			result.Err = err
			return result
		}
		if latest, ok := latestRequestStatusEntry(entries, "Обязательные пенсионные отчисления"); ok {
			result.Pension = newerRequestStatusEntry(result.Pension, latest)
		}
		if latest, ok := latestRequestStatusEntry(entries, "Выплата пенсий и пособий"); ok {
			result.Benefit = newerRequestStatusEntry(result.Benefit, latest)
		}
	}

	return result
}

func latestRequestStatusEntry(entries []requestStatusEntry, requestName string) (requestStatusEntry, bool) {
	var latest requestStatusEntry
	found := false
	for _, entry := range entries {
		if strings.TrimSpace(entry.Request) != requestName {
			continue
		}
		if !found || requestStatusEntryAfter(entry, latest) {
			latest = entry
			found = true
		}
	}
	return latest, found
}

func newerRequestStatusEntry(current, candidate requestStatusEntry) requestStatusEntry {
	if strings.TrimSpace(current.Request) == "" || requestStatusEntryAfter(candidate, current) {
		return candidate
	}
	return current
}

func requestStatusEntryAfter(left, right requestStatusEntry) bool {
	leftTime, leftOK := parseAdiletDateTime(left.CreatedDate)
	rightTime, rightOK := parseAdiletDateTime(right.CreatedDate)
	if leftOK && rightOK {
		return leftTime.After(rightTime)
	}
	if leftOK != rightOK {
		return leftOK
	}
	return strings.TrimSpace(left.CreatedDate) > strings.TrimSpace(right.CreatedDate)
}

func parseAdiletDateTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	layouts := []string{
		"2006-01-02T15:04:05.999Z07:00",
		"2006-01-02T15:04:05.999999999Z07:00",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func matchedPDFDocumentTitles(matches []matchedPDFDocument) []string {
	titles := make([]string, 0, len(matches))
	for _, match := range matches {
		titles = append(titles, match.Title)
	}
	return titles
}

func matchedPDFDocumentRef(match matchedPDFDocument) string {
	did := documentStringField(match.Doc, "did")
	ddocName := documentStringField(match.Doc, "ddocName")
	lang := strings.TrimSpace(match.Lang)
	if lang == "" {
		lang = "ru"
	}
	parts := []string{match.Title}
	if did != "" {
		parts = append(parts, "did="+did)
	}
	if ddocName != "" {
		parts = append(parts, "ddocName="+ddocName)
	}
	parts = append(parts, "lang="+lang)
	if did != "" && ddocName != "" {
		requestURL := baseURL + "/api/rest/execproc/doc/" + url.PathEscape(did) + "/" + url.PathEscape(ddocName) + "?lang=" + url.QueryEscape(lang)
		parts = append(parts, requestURL)
	}
	return strings.Join(parts, " | ")
}

func downloadPDFDocument(session string, doc map[string]any, lang string) ([]byte, error) {
	did := documentStringField(doc, "did")
	ddocName := documentStringField(doc, "ddocName")
	if did == "" || ddocName == "" {
		return nil, nil
	}
	if lang == "" {
		lang = "ru"
	}

	requestURL := baseURL + "/api/rest/execproc/doc/" + url.PathEscape(did) + "/" + url.PathEscape(ddocName) + "?lang=" + url.QueryEscape(lang)
	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}
	setExecProcHeaders(req, session)
	req.Header.Set("Accept", "application/json, text/plain, */*")

	client, err := getHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("скачивание PDF: %w", newHTTPStatusError(resp.StatusCode, data))
	}
	return data, nil
}

type pdfParseAPIResponse struct {
	Date        string `json:"date"`
	Basis       string `json:"basis"`
	FileSHA1    string `json:"fileSha1"`
	TextSHA1    string `json:"textSha1"`
	PageCount   int    `json:"pageCount"`
	TextPreview string `json:"textPreview"`
}

func parsePDFWithPythonAPI(data []byte) (pdfParseAPIResponse, error) {
	apiURL := strings.TrimSpace(os.Getenv("PDF_PARSE_API_URL"))
	if apiURL == "" {
		apiURL = defaultPDFParseAPI
	}

	payload, err := json.Marshal(map[string]string{
		"fileBase64": base64.StdEncoding.EncodeToString(data),
	})
	if err != nil {
		return pdfParseAPIResponse{}, err
	}

	sem := getPDFParseAPISemaphore()
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		sem <- struct{}{}
		parsed, err := parsePDFWithPythonAPIOnce(apiURL, payload)
		<-sem
		if err == nil {
			return parsed, nil
		}
		lastErr = err
		if !isRetryablePDFParseAPIError(err) || attempt == 3 {
			break
		}
		time.Sleep(time.Duration(attempt) * 750 * time.Millisecond)
	}

	return pdfParseAPIResponse{}, fmt.Errorf("Python PDF API недоступен (%s): %w", apiURL, lastErr)
}

func parsePDFWithPythonAPIOnce(apiURL string, payload []byte) (pdfParseAPIResponse, error) {
	req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewReader(payload))
	if err != nil {
		return pdfParseAPIResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 180 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return pdfParseAPIResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return pdfParseAPIResponse{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return pdfParseAPIResponse{}, newHTTPStatusError(resp.StatusCode, body)
	}

	var parsed pdfParseAPIResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return pdfParseAPIResponse{}, fmt.Errorf("Python PDF API вернул не JSON: %w", err)
	}
	return parsed, nil
}

func getPDFParseAPISemaphore() chan struct{} {
	pdfParseAPISemaphoreOnce.Do(func() {
		pdfParseAPISemaphore = make(chan struct{}, normalizePDFParseAPIWorkers(os.Getenv("PDF_PARSE_API_CONCURRENCY")))
	})
	return pdfParseAPISemaphore
}

func normalizePDFParseAPIWorkers(value string) int {
	workers := defaultPDFParseAPIWorkers
	if parsed, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && parsed > 0 {
		workers = parsed
	}
	if workers > maxPDFParseAPIWorkers {
		workers = maxPDFParseAPIWorkers
	}
	if workers < 1 {
		workers = 1
	}
	return workers
}

func isRetryablePDFParseAPIError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == http.StatusTooManyRequests || statusErr.StatusCode >= 500
	}
	return true
}

func documentStringField(doc map[string]any, key string) string {
	value, ok := doc[key]
	if !ok {
		return ""
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return ""
	}
	return text
}

func safeFileName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "file"
	}
	replacer := strings.NewReplacer(
		"\\", "_", "/", "_", ":", "_", "*", "_", "?", "_", `"`, "_",
		"<", "_", ">", "_", "|", "_", "\n", " ", "\r", " ", "\t", " ",
	)
	value = replacer.Replace(value)
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return "file"
	}
	return value
}

func buildFilesZipBytes(report []byte, reportName string, files []string) ([]byte, error) {
	var buffer bytes.Buffer
	zipWriter := zip.NewWriter(&buffer)

	reportWriter, err := zipWriter.Create(reportName)
	if err != nil {
		return nil, fmt.Errorf("не удалось добавить отчет в zip: %w", err)
	}
	if _, err := reportWriter.Write(report); err != nil {
		return nil, fmt.Errorf("не удалось записать отчет в zip: %w", err)
	}

	for _, fileName := range files {
		data, err := os.ReadFile(fileName)
		if err != nil {
			return nil, fmt.Errorf("не удалось прочитать %s: %w", fileName, err)
		}
		zipName := filepath.ToSlash(filepath.Join("pdf", filepath.Base(fileName)))
		writer, err := zipWriter.Create(zipName)
		if err != nil {
			return nil, fmt.Errorf("не удалось добавить %s в zip: %w", fileName, err)
		}
		if _, err := writer.Write(data); err != nil {
			return nil, fmt.Errorf("не удалось записать %s в zip: %w", fileName, err)
		}
	}

	if err := zipWriter.Close(); err != nil {
		return nil, fmt.Errorf("не удалось закрыть zip: %w", err)
	}
	return buffer.Bytes(), nil
}

func execProcJSONRequest(method, requestURL, session string, payload any, target any) error {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, requestURL, body)
	if err != nil {
		return err
	}
	setExecProcHeaders(req, session)

	client, err := getHTTPClient()
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return newHTTPStatusError(resp.StatusCode, data)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("не удалось разобрать JSON: %w", err)
	}
	return nil
}

func setExecProcHeaders(req *http.Request, session string) {
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "ru")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", cookieHeaderValue(session))
	req.Header.Set("Origin", "https://aisoip.adilet.gov.kz")
	req.Header.Set("Referer", "https://aisoip.adilet.gov.kz/cabinet/exec-productions")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("User-Agent", execProcUA)
	req.Header.Set("sec-ch-ua", `"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"`)
	req.Header.Set("sec-ch-ua-mobile", "?1")
	req.Header.Set("sec-ch-ua-platform", `"Android"`)
}

func extractExecProcIDs(value any) []string {
	seen := make(map[string]bool)
	ids := make([]string, 0)
	var walk func(any)

	add := func(value any) {
		text := numberIDText(value)
		if text == "" || text == "<nil>" || seen[text] {
			return
		}
		seen[text] = true
		ids = append(ids, text)
	}

	walk = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			for _, key := range []string{"execProcId", "execprocId", "exec_proc_id", "id"} {
				if item, ok := typed[key]; ok {
					add(item)
					break
				}
			}
			for _, item := range typed {
				switch item.(type) {
				case map[string]any, []any:
					walk(item)
				}
			}
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}

	walk(value)
	return ids
}

func extractExecProcIDForExecProcNum(value any, execProcNum string) string {
	target := normalizeExecProcNum(execProcNum)
	if target == "" {
		return ""
	}

	root, ok := value.(map[string]any)
	if !ok {
		return ""
	}

	content, ok := root["content"].([]any)
	if !ok {
		return ""
	}

	for _, item := range content {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if normalizeExecProcNum(firstAnyText(row, "execProcNum", "exec_proc_num")) != target {
			continue
		}
		for _, key := range []string{"execProcId", "execprocId", "exec_proc_id", "id"} {
			if item, ok := row[key]; ok {
				return numberIDText(item)
			}
		}
	}

	return ""
}

func normalizeExecProcNum(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func numberIDText(value any) string {
	switch typed := value.(type) {
	case json.Number:
		return strings.TrimSpace(typed.String())
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
	case float32:
		if typed == float32(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func pdfTitleMatchesTarget(title string) bool {
	return compactTitleContains(title, targetPDFTitle)
}

func pdfTitleMatchesKazakhTarget(title string) bool {
	return compactTitleContains(title, targetPDFTitleKZ)
}

func compactTitleContains(title, target string) bool {
	compactTitle := compactLower(title)
	compactTarget := compactLower(target)
	return compactTitle == compactTarget || strings.Contains(compactTitle, compactTarget)
}

func compactLower(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(value)), "")
}

func fetchPDFTitlesForNumberWithAuth(jobID string, index int, rowIndex int, number string) pdfTitleResult {
	for {
		session, err := waitForActiveJobSession(jobID)
		if err != nil {
			return pdfTitleResult{Index: index, RowIndex: rowIndex, Number: number, Err: err}
		}
		result := fetchPDFTitlesForNumber(index, rowIndex, number, session)
		if isAuthExpiredError(result.Err) {
			pausePDFJobForAuth(jobID)
			continue
		}
		return result
	}
}

func fetchPDFTitlesForExactExecProcWithAuth(jobID string, index int, rowIndex int, number, execProcNum string) pdfTitleResult {
	for {
		session, err := waitForActiveJobSession(jobID)
		if err != nil {
			return pdfTitleResult{Index: index, RowIndex: rowIndex, Number: number, ExecProcNum: execProcNum, Err: err}
		}
		result := fetchPDFTitlesForExactExecProc(index, rowIndex, number, execProcNum, session)
		if isAuthExpiredError(result.Err) {
			pausePDFJobForAuth(jobID)
			continue
		}
		return result
	}
}

func fetchRequestStatusForNumberWithAuth(jobID string, index int, number string) requestStatusResult {
	for {
		session, err := waitForActiveJobSession(jobID)
		if err != nil {
			return requestStatusResult{Index: index, Number: number, Err: err}
		}
		result := fetchRequestStatusForNumber(index, number, session)
		if isAuthExpiredError(result.Err) {
			pausePDFJobForAuth(jobID)
			continue
		}
		return result
	}
}

func fetchArrestInfo(baseURL, session, execProcNum string) (map[string]any, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("некорректный baseURL: %w", err)
	}

	u.Path = strings.TrimRight(u.Path, "/") + "/api/rest/claimant/arrestInfo"
	query := u.Query()
	query.Set("execProcNum", execProcNum)
	u.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("не удалось создать запрос: %w", err)
	}

	req.AddCookie(&http.Cookie{Name: "SESSION", Value: session})
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "ru")
	req.Header.Set("Referer", strings.TrimRight(baseURL, "/")+"/cabinet/claimant-arrests")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")

	client, err := getHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка запроса: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать ответ: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, newHTTPStatusError(resp.StatusCode, body)
	}

	var parsed map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("не удалось разобрать JSON: %w", err)
	}

	return parsed, nil
}

func processNumberWithAuth(jobID string, index int, number string, header []string) fetchResult {
	for {
		session, err := waitForActiveJobSession(jobID)
		if err != nil {
			return buildFetchErrorResult(index, number, header, err)
		}
		result := processNumber(index, number, session, header)
		if isAuthExpiredError(result.Err) {
			pausePDFJobForAuth(jobID)
			continue
		}
		return result
	}
}

func buildFetchErrorResult(index int, number string, header []string, err error) fetchResult {
	resultRow := make([]string, 0, len(header))
	for _, column := range exportColumns {
		if column.Path == "execProcNum" {
			resultRow = append(resultRow, number)
			continue
		}
		resultRow = append(resultRow, "")
	}
	resultRow = append(resultRow, "Ошибка", err.Error())
	return fetchResult{
		Index:     index,
		Number:    number,
		Err:       err,
		ResultRow: resultRow,
	}
}

func processNumber(index int, number, sessionKey string, header []string) fetchResult {
	resultRow := make([]string, 0, len(header))
	parsed, err := fetchArrestInfo(baseURL, sessionKey, number)
	if err != nil {
		return buildFetchErrorResult(index, number, header, err)
	}

	for _, column := range exportColumns {
		value := extractPathValue(parsed, column.Path)
		if column.Path == "execProcNum" && value == "" {
			value = number
		}
		resultRow = append(resultRow, value)
	}
	resultRow = append(resultRow, "OK", "")

	var unhandledEntryValue *unhandledEntry
	if unhandled := collectUnhandledData(parsed); len(unhandled) > 0 {
		unhandledEntryValue = &unhandledEntry{
			ExecProcNum:     firstNonEmpty(extractPathValue(parsed, "execProcNum"), number),
			DebtorFullName:  extractPathValue(parsed, "debtorFullName"),
			DebtorIinBin:    extractPathValue(parsed, "debtorIinBin"),
			UnhandledBlocks: unhandled,
		}
	}

	return fetchResult{
		Index:     index,
		Number:    number,
		Parsed:    parsed,
		Err:       nil,
		ResultRow: resultRow,
		Unhandled: unhandledEntryValue,
	}
}

func statusFromError(err error) string {
	if err != nil {
		return "Ошибка"
	}
	return "OK"
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func extractPathValue(data map[string]any, path string) string {
	var current any = data
	for _, part := range strings.Split(path, ".") {
		mapped, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current, ok = mapped[part]
		if !ok {
			return ""
		}
	}
	return stringifyValue(current)
}

func collectUnhandledData(parsed map[string]any) map[string]any {
	handledTopLevel := map[string]struct{}{
		"execProcNum":       {},
		"debtorFullName":    {},
		"debtorIinBin":      {},
		"recoveryAmount":    {},
		"recoveryAmountMrp": {},
		"collectedInfo":     {},
		"gcvpDetail":        {},
		"smsNotifLists":     {},
		"clEnisInfo":        {},
		"clFlUlRegInfo":     {},
		"tradeInfo":         {},
		"autoInfo":          {},
		"rnInfo":            {},
		"autoDrInfoDto":     {},
		"travelBanInfo":     {},
		"bankBanInfo":       {},
	}

	unhandled := make(map[string]any)
	for key, value := range parsed {
		if _, ok := handledTopLevel[key]; ok {
			continue
		}
		if hasMeaningfulValue(value) {
			unhandled[key] = value
		}
	}

	if gcvpRaw, ok := parsed["gcvpDetail"].(map[string]any); ok {
		extra := make(map[string]any)
		handledGCVPKeys := map[string]struct{}{
			"clGCVPInfo":               {},
			"clGCVPPaymentPensionDtos": {},
		}
		for key, value := range gcvpRaw {
			if _, ok := handledGCVPKeys[key]; !ok && hasMeaningfulValue(value) {
				extra[key] = value
			}
		}
		if hasMeaningfulValue(gcvpRaw["clGCVPInfo"]) {
			extra["clGCVPInfo"] = gcvpRaw["clGCVPInfo"]
		}
		if len(extra) > 0 {
			unhandled["gcvpDetail"] = extra
		}
	}

	if hasMeaningfulValue(parsed["tradeInfo"]) {
		unhandled["tradeInfo"] = parsed["tradeInfo"]
	}
	return unhandled
}

func hasMeaningfulValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		return len(typed) > 0
	case map[string]any:
		if len(typed) == 0 {
			return false
		}
		for _, item := range typed {
			if hasMeaningfulValue(item) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func buildUnhandledFileName() string {
	return "unhandled_data_" + time.Now().Format("20060102_150405") + ".json"
}

func datedXLSXFileName(base string) string {
	return fmt.Sprintf("%s_%s.xlsx", safeFileName(base), time.Now().Format("20060102_150405"))
}

func datedExecProcResultFileName(statuses []string) string {
	statusLabel := "Все"
	if len(statuses) > 0 {
		statusNames := make([]string, 0, len(statuses))
		for _, status := range statuses {
			statusNames = append(statusNames, execProcStatusFileNameLabel(status))
		}
		statusLabel = strings.Join(statusNames, "-")
	}
	return fmt.Sprintf("result_%s_%s.xlsx", safeFileName(statusLabel), time.Now().Format("20060102_150405"))
}

func execProcStatusFileNameLabel(status string) string {
	switch strings.TrimSpace(status) {
	case "1":
		return "На исполнении"
	case "2":
		return "Отказано в возбуждении"
	case "3":
		return "Возврат без исполнения"
	case "50":
		return "Окончено"
	case "51":
		return "Приостановлено"
	case "52":
		return "Направлено по территории"
	default:
		return "Все"
	}
}

func isProblemLog(message string) bool {
	return strings.HasPrefix(message, "[ERROR]") || strings.HasPrefix(message, "[WARN]")
}

func appendLine(fileName, line string) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(line + "\n")
	return err
}

func appendBankArrestRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["bankBanInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range bankArrestColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "arrestDate" || column.Path == "irDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendClEnisRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["clEnisInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range notaryBanColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "banDate" || column.Path == "unbanDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendGCVPRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	root, ok := parsed["gcvpDetail"].(map[string]any)
	if !ok {
		return rows
	}

	items, ok := root["clGCVPPaymentPensionDtos"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{
			execProcNum,
			debtorIinBin,
			debtorFullName,
			detectGCVPCategory(mapped),
		}
		for _, column := range gcvpColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "payDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendDriverLicenseRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	switch item := parsed["autoDrInfoDto"].(type) {
	case map[string]any:
		if len(item) == 0 {
			return rows
		}
		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range driverLicenseColumns {
			value := extractPathValue(item, column.Path)
			if column.Path == "expireDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	case []any:
		for _, raw := range item {
			mapped, ok := raw.(map[string]any)
			if !ok || len(mapped) == 0 {
				continue
			}
			row := []string{execProcNum, debtorIinBin, debtorFullName}
			for _, column := range driverLicenseColumns {
				value := extractPathValue(mapped, column.Path)
				if column.Path == "expireDate" {
					value = formatDisplayDate(value)
				}
				row = append(row, value)
			}
			rows = append(rows, row)
		}
	}

	return rows
}

func appendNotificationRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["smsNotifLists"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range notificationColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "statusDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendAutoInfoRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["autoInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range autoInfoColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "banDate" || column.Path == "unbanDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendTravelBanRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["travelBanInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range travelBanColumns {
			value := extractPathValue(mapped, column.Path)
			switch column.Path {
			case "notifDate", "banDate", "suspDate", "unbanDate":
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendRegistrationBanRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["clFlUlRegInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range registrationBanColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "banDate" || column.Path == "unbanDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendPropertyArrestRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["rnInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range propertyArrestColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "banDate" || column.Path == "unbanDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func detectGCVPCategory(item map[string]any) string {
	candidates := []string{
		extractPathValue(item, "type.name_ru"),
		extractPathValue(item, "type"),
		extractPathValue(item, "category.name_ru"),
		extractPathValue(item, "category"),
	}

	for _, candidate := range candidates {
		value := strings.TrimSpace(candidate)
		if value == "" {
			continue
		}

		lower := strings.ToLower(value)
		switch {
		case strings.Contains(lower, "pension"), strings.Contains(lower, "пенс"):
			return "Пенсионка"
		case strings.Contains(lower, "payment"), strings.Contains(lower, "плат"):
			return "Платеж"
		default:
			return value
		}
	}

	return "Платеж"
}

func formatDisplayDate(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
	}
	for _, format := range formats {
		parsed, err := time.Parse(format, value)
		if err == nil {
			return parsed.Format("02.01.2006")
		}
	}

	if len(value) >= len("2006-01-02") {
		prefix := value[:10]
		parsed, err := time.Parse("2006-01-02", prefix)
		if err == nil {
			return parsed.Format("02.01.2006")
		}
	}

	return value
}

func normalizeExcelDate(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
		"02.01.2006",
		"02/01/2006",
		"01/02/2006",
	}
	for _, format := range formats {
		parsed, err := time.Parse(format, value)
		if err == nil {
			return parsed.Format("2006-01-02")
		}
	}

	if len(value) >= len("2006-01-02") {
		prefix := value[:10]
		parsed, err := time.Parse("2006-01-02", prefix)
		if err == nil {
			return parsed.Format("2006-01-02")
		}
	}

	if serial, err := strconv.ParseFloat(strings.ReplaceAll(value, ",", "."), 64); err == nil && serial > 1 {
		base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
		return base.Add(time.Duration(serial*24) * time.Hour).Format("2006-01-02")
	}

	return ""
}

func stringifyValue(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		if typed {
			return "Да"
		}
		return "Нет"
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func buildXLSXBytes(sheets []sheetData) ([]byte, error) {
	var buffer bytes.Buffer
	zipWriter := zip.NewWriter(&buffer)

	files := map[string]string{
		"[Content_Types].xml":        contentTypesXML(len(sheets)),
		"_rels/.rels":                rootRelsXML(),
		"docProps/app.xml":           appXML(sheets),
		"docProps/core.xml":          coreXML(),
		"xl/workbook.xml":            workbookXML(sheets),
		"xl/_rels/workbook.xml.rels": workbookRelsXML(len(sheets)),
		"xl/styles.xml":              stylesXML(),
	}

	for idx, sheet := range sheets {
		files[fmt.Sprintf("xl/worksheets/sheet%d.xml", idx+1)] = worksheetXML(sheet.Rows)
	}

	for _, name := range orderedFileNames(files) {
		writer, err := zipWriter.Create(name)
		if err != nil {
			return nil, fmt.Errorf("не удалось добавить %s в архив: %w", name, err)
		}
		if _, err := writer.Write([]byte(files[name])); err != nil {
			return nil, fmt.Errorf("не удалось записать %s: %w", name, err)
		}
	}

	if err := zipWriter.Close(); err != nil {
		return nil, fmt.Errorf("не удалось закрыть Excel архив: %w", err)
	}

	return buffer.Bytes(), nil
}

func orderedFileNames(files map[string]string) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}

func contentTypesXML(sheetCount int) string {
	var overrides strings.Builder
	overrides.WriteString(`<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>`)
	overrides.WriteString(`<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>`)
	overrides.WriteString(`<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>`)
	overrides.WriteString(`<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>`)
	for i := 1; i <= sheetCount; i++ {
		overrides.WriteString(fmt.Sprintf(`<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`, i))
	}
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
		`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
		`<Default Extension="xml" ContentType="application/xml"/>` +
		overrides.String() +
		`</Types>`
}

func rootRelsXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
		`<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>` +
		`<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>` +
		`<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>` +
		`</Relationships>`
}

func appXML(sheets []sheetData) string {
	var titles strings.Builder
	for _, sheet := range sheets {
		titles.WriteString(`<vt:lpstr>` + xmlEscape(sheet.Name) + `</vt:lpstr>`)
	}
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">` +
		`<Application>Go</Application>` +
		`<TitlesOfParts><vt:vector size="` + strconv.Itoa(len(sheets)) + `" baseType="lpstr">` + titles.String() + `</vt:vector></TitlesOfParts>` +
		`</Properties>`
}

func coreXML() string {
	now := time.Now().UTC().Format(time.RFC3339)
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">` +
		`<dc:creator>Codex</dc:creator>` +
		`<cp:lastModifiedBy>Codex</cp:lastModifiedBy>` +
		`<dcterms:created xsi:type="dcterms:W3CDTF">` + now + `</dcterms:created>` +
		`<dcterms:modified xsi:type="dcterms:W3CDTF">` + now + `</dcterms:modified>` +
		`</cp:coreProperties>`
}

func workbookXML(sheets []sheetData) string {
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	builder.WriteString(`<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>`)
	for idx, sheet := range sheets {
		builder.WriteString(fmt.Sprintf(`<sheet name="%s" sheetId="%d" r:id="rId%d"/>`, xmlEscape(safeSheetName(sheet.Name, idx+1)), idx+1, idx+1))
	}
	builder.WriteString(`</sheets></workbook>`)
	return builder.String()
}

func workbookRelsXML(sheetCount int) string {
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	builder.WriteString(`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`)
	for i := 1; i <= sheetCount; i++ {
		builder.WriteString(fmt.Sprintf(`<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>`, i, i))
	}
	builder.WriteString(fmt.Sprintf(`<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>`, sheetCount+1))
	builder.WriteString(`</Relationships>`)
	return builder.String()
}

func stylesXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">` +
		`<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>` +
		`<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>` +
		`<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>` +
		`<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>` +
		`<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>` +
		`<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>` +
		`</styleSheet>`
}

func worksheetXML(rows [][]string) string {
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	builder.WriteString(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`)
	for rIdx, row := range rows {
		builder.WriteString(fmt.Sprintf(`<row r="%d">`, rIdx+1))
		for cIdx, cell := range row {
			ref := columnName(cIdx+1) + strconv.Itoa(rIdx+1)
			styleID := "0"
			if rIdx == 0 {
				styleID = "1"
			}
			builder.WriteString(fmt.Sprintf(`<c r="%s" t="inlineStr" s="%s"><is><t xml:space="preserve">%s</t></is></c>`, ref, styleID, xmlEscape(cell)))
		}
		builder.WriteString(`</row>`)
	}
	builder.WriteString(`</sheetData></worksheet>`)
	return builder.String()
}

func safeSheetName(name string, index int) string {
	replacer := strings.NewReplacer("\\", "_", "/", "_", "*", "_", "[", "_", "]", "_", ":", "_", "?", "_")
	name = strings.TrimSpace(replacer.Replace(name))
	if name == "" {
		name = fmt.Sprintf("Лист%d", index)
	}
	runes := []rune(name)
	if len(runes) > 31 {
		return string(runes[:31])
	}
	return name
}

func columnName(n int) string {
	if n <= 0 {
		return ""
	}
	var result []byte
	for n > 0 {
		n--
		result = append([]byte{byte('A' + n%26)}, result...)
		n /= 26
	}
	return string(result)
}

func xmlEscape(value string) string {
	var buffer bytes.Buffer
	_ = xml.EscapeText(&buffer, []byte(value))
	return buffer.String()
}

func upgradeToWebSocket(w http.ResponseWriter, r *http.Request) (io.ReadWriteCloser, error) {
	if !headerContainsToken(r.Header, "Connection", "upgrade") || !headerContainsToken(r.Header, "Upgrade", "websocket") {
		return nil, errors.New("ожидалось websocket upgrade соединение")
	}

	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if key == "" {
		return nil, errors.New("отсутствует Sec-WebSocket-Key")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("server does not support hijacking")
	}

	conn, buf, err := hijacker.Hijack()
	if err != nil {
		return nil, fmt.Errorf("не удалось перехватить соединение: %w", err)
	}

	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + computeWebSocketAccept(key) + "\r\n\r\n"

	if _, err := buf.WriteString(response); err != nil {
		conn.Close()
		return nil, err
	}
	if err := buf.Flush(); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func headerContainsToken(header http.Header, key, token string) bool {
	for _, value := range header.Values(key) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func computeWebSocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

type wsFrame struct {
	final   bool
	opcode  byte
	payload []byte
}

func readClientTextFrame(r io.Reader) ([]byte, error) {
	var message []byte
	var started bool

	for {
		frame, err := readClientFrame(r)
		if err != nil {
			return nil, err
		}

		switch frame.opcode {
		case 0x8:
			return nil, io.EOF
		case 0x9, 0xA:
			if !frame.final {
				return nil, errors.New("fragmented control websocket frames are not supported")
			}
			continue
		case 0x1:
			if started {
				return nil, errors.New("получено новое websocket сообщение до завершения предыдущего")
			}
			started = true
		case 0x0:
			if !started {
				return nil, errors.New("получен continuation websocket frame без начального текстового сообщения")
			}
		default:
			return nil, errors.New("поддерживаются только текстовые websocket сообщения")
		}

		if int64(len(message))+int64(len(frame.payload)) > 128*1024*1024 {
			return nil, errors.New("слишком большой websocket payload")
		}
		message = append(message, frame.payload...)

		if frame.final {
			return message, nil
		}
	}
}

func readClientFrame(r io.Reader) (wsFrame, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return wsFrame{}, err
	}

	final := header[0]&0x80 != 0
	opcode := header[0] & 0x0F
	if header[1]&0x80 == 0 {
		return wsFrame{}, errors.New("client websocket frame must be masked")
	}

	payloadLen := int64(header[1] & 0x7F)
	switch payloadLen {
	case 126:
		extended := make([]byte, 2)
		if _, err := io.ReadFull(r, extended); err != nil {
			return wsFrame{}, err
		}
		payloadLen = int64(binary.BigEndian.Uint16(extended))
	case 127:
		extended := make([]byte, 8)
		if _, err := io.ReadFull(r, extended); err != nil {
			return wsFrame{}, err
		}
		payloadLen = int64(binary.BigEndian.Uint64(extended))
	}

	if payloadLen > 128*1024*1024 {
		return wsFrame{}, errors.New("слишком большой websocket payload")
	}

	mask := make([]byte, 4)
	if _, err := io.ReadFull(r, mask); err != nil {
		return wsFrame{}, err
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return wsFrame{}, err
	}

	for i := range payload {
		payload[i] ^= mask[i%4]
	}

	return wsFrame{
		final:   final,
		opcode:  opcode,
		payload: payload,
	}, nil
}

func writeServerJSON(w io.Writer, message wsMessage) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return writeServerTextFrame(w, payload)
}

func writeServerTextFrame(w io.Writer, payload []byte) error {
	header := []byte{0x81}
	length := len(payload)

	switch {
	case length <= 125:
		header = append(header, byte(length))
	case length <= 65535:
		header = append(header, 126, byte(length>>8), byte(length))
	default:
		extended := make([]byte, 8)
		binary.BigEndian.PutUint64(extended, uint64(length))
		header = append(header, 127)
		header = append(header, extended...)
	}

	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func init() {
	log.SetOutput(os.Stdout)
}

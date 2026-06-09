package main

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestIsAuthExpiredErrorDetectsWrappedUnauthorized(t *testing.T) {
	err := fmt.Errorf("request failed: %w", newHTTPStatusError(http.StatusUnauthorized, []byte("expired")))

	if !isAuthExpiredError(err) {
		t.Fatal("expected wrapped 401 to be treated as expired auth")
	}
}

func TestIsAuthExpiredErrorIgnoresOtherStatuses(t *testing.T) {
	err := fmt.Errorf("request failed: %w", newHTTPStatusError(http.StatusForbidden, []byte("forbidden")))

	if isAuthExpiredError(err) {
		t.Fatal("expected non-401 status to be ignored")
	}
}

func TestPDFTitleMatchingSupportsRussianAndKazakhTargets(t *testing.T) {
	if !pdfTitleMatchesTarget("Постановление о прекращении исполнительного производства") {
		t.Fatal("expected Russian target title to match")
	}
	if !pdfTitleMatchesKazakhTarget("Қаулы атқарушылық іс жүргізуді тоқтату туралы") {
		t.Fatal("expected Kazakh target title to match")
	}
	if pdfTitleMatchesTarget("Қаулы атқарушылық іс жүргізуді тоқтату туралы") {
		t.Fatal("expected Russian matcher to ignore Kazakh title")
	}
}

func TestPDFParseJobsFromRowsUsesFirstColumnWithoutStatusColumn(t *testing.T) {
	rows := [][]string{
		{"Номер исполнительного производства"},
		{"12345"},
		{"67890", "На исполнении"},
		{""},
	}

	jobs := pdfParseJobsFromRows(rows)

	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	if jobs[0].Number != "12345" || jobs[0].RowIndex != 1 {
		t.Fatalf("unexpected first job: %+v", jobs[0])
	}
	if jobs[1].Number != "67890" || jobs[1].RowIndex != 2 {
		t.Fatalf("unexpected second job: %+v", jobs[1])
	}
}

func TestPDFParseColumnsIncludeProcessingStatus(t *testing.T) {
	rows := appendPDFParseColumns([][]string{
		{"Номер исполнительного производства"},
		{"12345"},
	})

	if got := rows[0][len(rows[0])-1]; got != "Статус обработки" {
		t.Fatalf("expected status header, got %q", got)
	}

	status := pdfParseStatus(pdfTitleResult{
		DecreeDate: "01.01.2026",
		LegalBasis: "подпунктом 7 пункта 1 статьи 47",
	})
	setPDFParseColumns(rows, 1, "01.01.2026", "подпунктом 7 пункта 1 статьи 47", status)
	if got := rows[1][len(rows[1])-1]; got != "OK" {
		t.Fatalf("expected OK status, got %q", got)
	}
}

func TestPDFParseStatusReportsErrors(t *testing.T) {
	got := pdfParseStatus(pdfTitleResult{Err: errors.New("PDF недоступен")})

	if got != "Ошибка: PDF недоступен" {
		t.Fatalf("unexpected status: %q", got)
	}
}

func TestNormalizePDFParseAPIWorkers(t *testing.T) {
	if got := normalizePDFParseAPIWorkers(""); got != defaultPDFParseAPIWorkers {
		t.Fatalf("expected default workers, got %d", got)
	}
	if got := normalizePDFParseAPIWorkers("999"); got != maxPDFParseAPIWorkers {
		t.Fatalf("expected max workers, got %d", got)
	}
	if got := normalizePDFParseAPIWorkers("2"); got != 2 {
		t.Fatalf("expected explicit workers, got %d", got)
	}
}

func TestLatestRequestStatusEntryUsesNewestResponseDate(t *testing.T) {
	entries := []requestStatusEntry{
		{Request: "Обязательные пенсионные отчисления", State: "старый", ResponseDate: "2025-04-08T17:52:11.700+05:00"},
		{Request: "Выплата пенсий и пособий", State: "другой", ResponseDate: "2026-05-08T18:02:09.704+05:00"},
		{Request: "Обязательные пенсионные отчисления", State: "новый", ResponseDate: "2026-05-09T00:04:19.622+05:00"},
	}

	latest, ok := latestRequestStatusEntry(entries, "Обязательные пенсионные отчисления")

	if !ok {
		t.Fatal("expected entry to be found")
	}
	if latest.State != "новый" {
		t.Fatalf("expected newest entry, got %+v", latest)
	}
}

func TestFormatDisplayDateDropsTime(t *testing.T) {
	got := formatDisplayDate("2026-04-17T12:54:24.052+05:00")
	want := "17.04.2026"

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

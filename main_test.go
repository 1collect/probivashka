package main

import (
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

func TestStrictExecProcPairJobsFromRowsUsesTwoColumns(t *testing.T) {
	rows := [][]string{
		{"Исполнительный документ", "Исполнительное производство"},
		{"DOC-1", "PROC-1"},
	}

	jobs, err := strictExecProcPairJobsFromRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Number != "DOC-1" || jobs[0].ExecProcNum != "PROC-1" || jobs[0].RowIndex != 1 {
		t.Fatalf("unexpected job: %+v", jobs[0])
	}
}

func TestStrictExecProcPairJobsFromRowsRejectsExtraColumn(t *testing.T) {
	_, err := strictExecProcPairJobsFromRows([][]string{{"DOC-1", "PROC-1", "лишнее"}})
	if err == nil {
		t.Fatal("expected extra column to be rejected")
	}
}

func TestStrictIINExecProcPairJobsFromRowsUsesIINAndExecProc(t *testing.T) {
	rows := [][]string{
		{"ИИН", "Исполнительное производство"},
		{"031001501639", "PROC-1"},
	}

	jobs, err := strictIINExecProcPairJobsFromRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Number != "031001501639" || jobs[0].ExecProcNum != "PROC-1" || jobs[0].RowIndex != 1 {
		t.Fatalf("unexpected job: %+v", jobs[0])
	}
}

func TestStrictIINExecProcPairJobsFromRowsRejectsInvalidIIN(t *testing.T) {
	_, err := strictIINExecProcPairJobsFromRows([][]string{{"123", "PROC-1"}})
	if err == nil {
		t.Fatal("expected invalid IIN to be rejected")
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

func TestLatestRequestStatusEntryUsesNewestCreatedDate(t *testing.T) {
	entries := []requestStatusEntry{
		{Request: "Обязательные пенсионные отчисления", State: "старый", CreatedDate: "2025-04-08T17:52:11.700+05:00", ResponseDate: "2026-05-09T00:04:19.622+05:00"},
		{Request: "Выплата пенсий и пособий", State: "другой", CreatedDate: "2026-05-08T18:02:09.704+05:00", ResponseDate: "2026-05-08T18:02:09.704+05:00"},
		{Request: "Обязательные пенсионные отчисления", State: "новый", CreatedDate: "2026-05-09T00:04:19.622+05:00", ResponseDate: "2025-04-08T17:52:11.700+05:00"},
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

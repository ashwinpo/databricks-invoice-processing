/**
 * Invoice Review Queue + Split-Pane Review UI.
 *
 * Two views managed by useState<"queue" | "review">:
 *   Queue:  upload area, invoice table with status badges, export buttons
 *   Review: left pane (PDF + bounding boxes), right pane (editable field form)
 *
 * Processing flow per invoice:
 *   upload -> ai_parse_document (poll until ready) -> ai_query (extract fields) -> queue
 *
 * Field config is fetched from /api/invoice-fields (sourced from invoice_fields.yaml).
 * All state is local — no external state management.
 */
"use client";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useState, useRef, useEffect, useCallback } from "react";
import Link from "next/link";
import {
  ArrowLeft,
  Upload,
  FileText,
  Loader2,
  Check,
  ChevronLeft,
  ChevronRight,
  SkipForward,
  ZoomIn,
  ZoomOut,
  Settings,
  X,
  Download,
  AlertTriangle,
  RotateCcw,
} from "lucide-react";
import { apiCall, getBaseUrl } from "@/lib/api-config";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface InvoiceField {
  name: string;
  label: string;
  description: string;
  type: string;
}

interface Invoice {
  file_path: string;
  filename: string;
  status: "uploading" | "parsing" | "extracting" | "extracted" | "confirmed" | "error";
  extracted_fields: Record<string, string | null> | null;
  confirmed_fields: Record<string, string | null> | null;
  error?: string;
  // local-only fields for the upload+process flow
  file?: File;
  ucPath?: string;
}

interface ProcessingError {
  file_path: string;
  filename: string;
  stage: string;
  error_message: string;
  created_at: string;
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function InvoiceProcessingPage() {
  const [view, setView] = useState<"queue" | "review">("queue");
  const [invoices, setInvoices] = useState<Invoice[]>([]);
  const [activeIndex, setActiveIndex] = useState(0);
  const [fieldConfig, setFieldConfig] = useState<InvoiceField[]>([]);
  const [editedFields, setEditedFields] = useState<Record<string, string>>({});
  const [visualization, setVisualization] = useState<Record<string, { image_base64: string; elements: any[] }>>({});
  const [vizLoading, setVizLoading] = useState(false);
  const [currentPage, setCurrentPage] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [zoom, setZoom] = useState(100);
  const [confirmLoading, setConfirmLoading] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [processingErrors, setProcessingErrors] = useState<ProcessingError[]>([]);
  const [retryingPaths, setRetryingPaths] = useState<Set<string>>(new Set());
  const [showErrorsPanel, setShowErrorsPanel] = useState(false);
  const [rateLimits, setRateLimits] = useState<{
    max_documents_per_day: number;
    max_files_per_upload: number;
    remaining_today: number | null;
  } | null>(null);
  const [rateLimitError, setRateLimitError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Load field config on mount + invoice queue
  useEffect(() => {
    apiCall("/api/invoice-fields").then((res) => {
      if (res.fields) setFieldConfig(res.fields);
    }).catch(console.error);

    loadQueue();
    loadErrors();
    loadRateLimits();
  }, []);

  const loadQueue = async () => {
    try {
      const res = await apiCall("/api/invoice-queue");
      if (res.success && res.invoices) {
        setInvoices((prev) => {
          // Merge server invoices with any local in-progress ones
          const localInProgress = prev.filter(
            (inv) => ["uploading", "parsing", "extracting"].includes(inv.status)
          );
          const serverPaths = new Set(res.invoices.map((i: Invoice) => i.file_path));
          const uniqueLocal = localInProgress.filter((i) => !serverPaths.has(i.file_path));
          return [...uniqueLocal, ...res.invoices];
        });
      }
    } catch (e) {
      console.error("Failed to load queue:", e);
    }
  };

  const loadRateLimits = async () => {
    try {
      const res = await apiCall("/api/rate-limits");
      setRateLimits(res);
    } catch {
      // Non-critical
    }
  };

  const loadErrors = async () => {
    try {
      const res = await apiCall("/api/processing-errors");
      if (res.success && res.errors) {
        setProcessingErrors(res.errors);
        if (res.errors.length === 0) setShowErrorsPanel(false);
      }
    } catch (e) {
      console.error("Failed to load processing errors:", e);
    }
  };

  const handleRetry = async (filePath: string) => {
    setRetryingPaths((prev) => new Set(prev).add(filePath));
    try {
      const res = await apiCall("/api/retry-invoice", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_path: filePath }),
      });
      if (res.success && res.fields) {
        // Update the invoice in the queue with new extracted fields
        setInvoices((prev) => {
          const exists = prev.some((i) => i.file_path === filePath);
          if (exists) {
            return prev.map((i) =>
              i.file_path === filePath
                ? { ...i, status: "extracted" as const, extracted_fields: res.fields, error: undefined }
                : i
            );
          }
          return [
            {
              file_path: filePath,
              filename: filePath.split("/").pop() || filePath,
              status: "extracted" as const,
              extracted_fields: res.fields,
              confirmed_fields: null,
            },
            ...prev,
          ];
        });
      }
      // Reload errors and queue regardless of result
      await Promise.all([loadErrors(), loadQueue()]);
    } catch (e) {
      console.error("Retry failed:", e);
    } finally {
      setRetryingPaths((prev) => {
        const next = new Set(prev);
        next.delete(filePath);
        return next;
      });
    }
  };

  const handleDismissError = async (filePath: string, stage: string) => {
    try {
      await apiCall("/api/dismiss-error", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_path: filePath, stage }),
      });
      await loadErrors();
    } catch (e) {
      console.error("Dismiss error failed:", e);
    }
  };

  // -------------------------------------------------------------------------
  // Upload & Process
  // -------------------------------------------------------------------------

  const handleFileSelect = () => fileInputRef.current?.click();

  const handleFilesChosen = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files || files.length === 0) return;

    const fileArray = Array.from(files);
    // Reset input so same files can be selected again
    e.target.value = "";

    await uploadAndProcessBatch(fileArray);
  };

  const handleDrop = async (e: React.DragEvent) => {
    e.preventDefault();
    const files = Array.from(e.dataTransfer.files).filter(
      (f) => f.type === "application/pdf" || f.name.toLowerCase().endsWith(".pdf")
    );
    if (files.length > 0) await uploadAndProcessBatch(files);
  };

  const uploadAndProcessBatch = async (files: File[]) => {
    setRateLimitError(null);

    // Step 1: Add all files to the queue immediately so the user sees them
    const pending: { file: File; tempPath: string }[] = files.map((file) => ({
      file,
      tempPath: `__pending__/${file.name}`,
    }));

    setInvoices((prev) => [
      ...pending.map(({ file, tempPath }) => ({
        file_path: tempPath,
        filename: file.name,
        status: "uploading" as const,
        extracted_fields: null,
        confirmed_fields: null,
        file,
      })),
      ...prev,
    ]);

    // Step 2: Upload all files in one batch request
    const formData = new FormData();
    for (const { file } of pending) {
      formData.append("files", file);
    }

    let uploadedFiles: { name: string; path: string }[];
    try {
      const uploadRes = await apiCall("/api/upload-to-uc", { method: "POST", body: formData }).catch((err) => {
        if (err?.message?.includes("429") || err?.message?.includes("limit")) {
          const detail = err?.message || "Rate limit exceeded";
          setRateLimitError(detail);
          throw new Error(detail);
        }
        throw err;
      });
      uploadedFiles = uploadRes.uploaded_files || [];
    } catch (err: any) {
      const errMsg = err?.message || String(err);
      setInvoices((prev) =>
        prev.map((inv) =>
          inv.file_path.startsWith("__pending__/")
            ? { ...inv, status: "error", error: errMsg }
            : inv
        )
      );
      loadErrors();
      loadRateLimits();
      return;
    }

    // Map uploaded paths back to pending entries by index (order is preserved).
    // Can't match by name because the backend may rename duplicates (e.g. file_1.pdf).
    const uploaded: { tempPath: string; ucPath: string; filename: string }[] = [];
    for (let i = 0; i < pending.length; i++) {
      const uf = uploadedFiles[i];
      if (uf?.path) {
        uploaded.push({ tempPath: pending[i].tempPath, ucPath: uf.path, filename: uf.name });
      }
    }

    const allUcPaths = uploaded.map((u) => u.ucPath);

    // Step 3: Parse ALL files in one ai_parse_document call
    setInvoices((prev) =>
      prev.map((inv) => {
        const match = uploaded.find((u) => u.tempPath === inv.file_path);
        return match
          ? { ...inv, file_path: match.ucPath, ucPath: match.ucPath, status: "parsing" as const }
          : inv;
      })
    );

    loadRateLimits();

    // Fire single parse request for all files
    apiCall("/api/write-to-delta-table", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ file_paths: allUcPaths }),
    }).catch(() => {});

    // Poll until ALL files are parsed
    let allParsed = false;
    for (let attempt = 0; attempt < 90; attempt++) {
      await sleep(5000);
      try {
        const queryRes = await apiCall("/api/query-delta-table", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ file_paths: allUcPaths }),
        });
        if (queryRes.success && queryRes.data) {
          // Check that we have results for every file
          const parsedPaths = new Set(queryRes.data.map((d: any) => d.path));
          const allFound = allUcPaths.every((p) => {
            const dbfs = p.startsWith("/Volumes/") ? `dbfs:${p}` : p;
            return parsedPaths.has(dbfs);
          });
          if (allFound) {
            allParsed = true;
            break;
          }
        }
      } catch {
        // Keep polling
      }
    }

    if (!allParsed) {
      setInvoices((prev) =>
        prev.map((inv) =>
          allUcPaths.includes(inv.file_path) && inv.status === "parsing"
            ? { ...inv, status: "error", error: "Document parsing timed out" }
            : inv
        )
      );
      loadErrors();
      loadRateLimits();
      return;
    }

    // Step 4: Extract ALL fields in one ai_query call
    setInvoices((prev) =>
      prev.map((inv) =>
        allUcPaths.includes(inv.file_path)
          ? { ...inv, status: "extracting" }
          : inv
      )
    );

    let batchExtractRes = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        batchExtractRes = await apiCall("/api/batch-extract-fields", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ file_paths: allUcPaths }),
        });
        if (batchExtractRes.success) break;
      } catch (e) {
        console.warn(`Batch extract attempt ${attempt + 1}/3 failed:`, e);
        if (attempt < 2) await sleep(5000 * (attempt + 1));
      }
    }

    if (batchExtractRes?.success) {
      // Apply successes
      const successMap = new Map(
        (batchExtractRes.results || []).map((r: any) => [r.file_path, r.fields])
      );
      const errorMap = new Map(
        (batchExtractRes.errors || []).map((e: any) => [e.file_path, e.error])
      );

      setInvoices((prev) =>
        prev.map((inv) => {
          if (!allUcPaths.includes(inv.file_path)) return inv;
          const fields = successMap.get(inv.file_path);
          const error = errorMap.get(inv.file_path);
          if (fields) {
            return { ...inv, status: "extracted" as const, extracted_fields: fields };
          }
          // Extraction error but doc is parsed — let user review manually
          return {
            ...inv,
            status: "extracted" as const,
            extracted_fields: {},
            error: error || "Field extraction failed",
          };
        })
      );
    } else {
      // Total failure — mark all as extracted with empty fields for manual review
      const errMsg = batchExtractRes?.errors?.[0]?.error || "Batch extraction failed";
      setInvoices((prev) =>
        prev.map((inv) =>
          allUcPaths.includes(inv.file_path)
            ? { ...inv, status: "extracted" as const, extracted_fields: {}, error: errMsg }
            : inv
        )
      );
    }

    loadQueue();
    loadErrors();
    loadRateLimits();
  };

  // -------------------------------------------------------------------------
  // Review
  // -------------------------------------------------------------------------

  const openReview = useCallback(
    async (index: number) => {
      const inv = invoices[index];
      if (!inv || !["extracted", "confirmed"].includes(inv.status)) return;

      setActiveIndex(index);
      setView("review");
      setCurrentPage(0);
      setVisualization({});
      setVizLoading(true);

      // Pre-fill form with extracted or confirmed fields
      const fields = inv.confirmed_fields || inv.extracted_fields || {};
      setEditedFields(
        Object.fromEntries(
          fieldConfig.map((f) => [f.name, fields[f.name] ?? ""])
        )
      );

      // Load page metadata
      try {
        const metaRes = await apiCall("/api/page-metadata", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ file_paths: [inv.file_path] }),
        });
        setTotalPages(metaRes.total_pages || 0);
      } catch {
        setTotalPages(0);
      }

      // Load first page visualization
      await loadVisualization(inv.file_path, 0);
    },
    [invoices, fieldConfig]
  );

  const loadVisualization = async (filePath: string, pageNum: number) => {
    setVizLoading(true);
    try {
      const res = await apiCall("/api/visualize-page", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_path: filePath, page_number: pageNum }),
      });
      if (res.success && res.visualizations) {
        setVisualization(res.visualizations);
      }
    } catch (e) {
      console.error("Visualization failed:", e);
    } finally {
      setVizLoading(false);
    }
  };

  const changePage = (delta: number) => {
    const newPage = currentPage + delta;
    if (newPage < 0 || newPage >= totalPages) return;
    setCurrentPage(newPage);
    const inv = invoices[activeIndex];
    if (inv) loadVisualization(inv.file_path, newPage);
  };

  const handleConfirm = async (advance: boolean) => {
    const inv = invoices[activeIndex];
    if (!inv) return;

    setConfirmLoading(true);
    try {
      await apiCall("/api/confirm-invoice", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_path: inv.file_path, fields: editedFields }),
      });

      setInvoices((prev) =>
        prev.map((i) =>
          i.file_path === inv.file_path
            ? { ...i, status: "confirmed", confirmed_fields: { ...editedFields } }
            : i
        )
      );

      if (advance) {
        // Find next unconfirmed invoice
        const nextIdx = invoices.findIndex(
          (i, idx) => idx > activeIndex && i.status === "extracted"
        );
        if (nextIdx !== -1) {
          openReview(nextIdx);
        } else {
          setView("queue");
          loadQueue();
        }
      } else {
        loadQueue();
      }
    } catch (e) {
      console.error("Confirm failed:", e);
    } finally {
      setConfirmLoading(false);
    }
  };

  const skipToNext = () => {
    const nextIdx = invoices.findIndex(
      (i, idx) => idx > activeIndex && i.status === "extracted"
    );
    if (nextIdx !== -1) {
      openReview(nextIdx);
    } else {
      setView("queue");
    }
  };

  // -------------------------------------------------------------------------
  // Export
  // -------------------------------------------------------------------------

  const handleExport = async (format: "csv" | "xlsx") => {
    try {
      const baseUrl = getBaseUrl();
      const res = await fetch(`${baseUrl}/api/export-invoices?format=${format}`);
      if (!res.ok) throw new Error(`Export failed (${res.status})`);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `invoices_export.${format}`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (e) {
      console.error("Export failed:", e);
    }
  };

  // -------------------------------------------------------------------------
  // Render
  // -------------------------------------------------------------------------

  const extractedCount = invoices.filter((i) => i.status === "extracted").length;
  const confirmedCount = invoices.filter((i) => i.status === "confirmed").length;
  const processingCount = invoices.filter((i) =>
    ["uploading", "parsing", "extracting"].includes(i.status)
  ).length;
  const errorCount = processingErrors.length;

  if (view === "review") {
    const inv = invoices[activeIndex];
    const vizKeys = Object.keys(visualization);
    const vizData = vizKeys.length > 0 ? visualization[vizKeys[0]] : null;
    const reviewableInvoices = invoices.filter((i) => ["extracted", "confirmed"].includes(i.status));
    const reviewIdx = reviewableInvoices.findIndex((i) => i.file_path === inv?.file_path);

    return (
      <div className="flex flex-col h-screen bg-gray-50">
        {/* Top bar */}
        <div className="bg-white border-b px-4 py-3 flex items-center justify-between shrink-0">
          <div className="flex items-center gap-3">
            <Button variant="ghost" size="sm" onClick={() => { setView("queue"); loadQueue(); }}>
              <ArrowLeft className="h-4 w-4 mr-1" /> Queue
            </Button>
            <span className="text-sm font-medium text-gray-700">
              {inv?.filename}
            </span>
            {reviewableInvoices.length > 0 && (
              <span className="text-xs text-gray-400">
                {reviewIdx + 1} of {reviewableInvoices.length}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <StatusBadge status={inv?.status || "extracted"} />
          </div>
        </div>

        {/* Split pane */}
        <div className="flex flex-1 overflow-hidden">
          {/* Left: PDF visualization */}
          <div className="w-1/2 border-r flex flex-col bg-gray-100">
            {/* Page nav + zoom */}
            <div className="bg-white border-b px-3 py-2 flex items-center justify-between shrink-0">
              <div className="flex items-center gap-2">
                <Button
                  variant="outline" size="sm"
                  disabled={currentPage <= 0}
                  onClick={() => changePage(-1)}
                >
                  <ChevronLeft className="h-4 w-4" />
                </Button>
                <span className="text-sm text-gray-600">
                  Page {currentPage + 1} of {totalPages || 1}
                </span>
                <Button
                  variant="outline" size="sm"
                  disabled={currentPage >= totalPages - 1}
                  onClick={() => changePage(1)}
                >
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </div>
              <div className="flex items-center gap-1">
                <Button variant="ghost" size="sm" onClick={() => setZoom((z) => Math.max(25, z - 25))}>
                  <ZoomOut className="h-4 w-4" />
                </Button>
                <span className="text-xs text-gray-500 w-12 text-center">{zoom}%</span>
                <Button variant="ghost" size="sm" onClick={() => setZoom((z) => Math.min(300, z + 25))}>
                  <ZoomIn className="h-4 w-4" />
                </Button>
                <Button variant="ghost" size="sm" onClick={() => setZoom(100)}>
                  Fit
                </Button>
              </div>
            </div>

            {/* Image */}
            <div className="flex-1 overflow-auto p-4 flex items-start justify-center">
              {vizLoading ? (
                <div className="flex items-center gap-2 text-gray-500 mt-20">
                  <Loader2 className="h-5 w-5 animate-spin" />
                  Loading page...
                </div>
              ) : vizData ? (
                <img
                  src={`data:image/png;base64,${vizData.image_base64}`}
                  alt={`Page ${currentPage + 1}`}
                  style={{ width: `${zoom}%`, maxWidth: "none" }}
                  className="shadow-lg"
                />
              ) : (
                <div className="text-gray-400 mt-20">No visualization available</div>
              )}
            </div>
          </div>

          {/* Right: Field editor */}
          <div className="w-1/2 flex flex-col">
            <div className="bg-white border-b px-4 py-3 shrink-0">
              <h2 className="text-sm font-semibold text-gray-700">Extracted Fields</h2>
              <p className="text-xs text-gray-400 mt-0.5">Review and correct the AI-extracted values</p>
            </div>

            <div className="flex-1 overflow-auto px-4 py-3">
              <div className="space-y-3">
                {fieldConfig.map((field) => (
                  <div key={field.name}>
                    <label className="block text-xs font-medium text-gray-600 mb-1">
                      {field.label}
                      <span className="text-gray-400 font-normal ml-2">{field.type}</span>
                    </label>
                    <input
                      type={field.type === "date" ? "date" : "text"}
                      value={editedFields[field.name] || ""}
                      onChange={(e) =>
                        setEditedFields((prev) => ({ ...prev, [field.name]: e.target.value }))
                      }
                      placeholder={field.description}
                      className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-white"
                    />
                  </div>
                ))}
              </div>
            </div>

            {/* Action buttons */}
            <div className="bg-white border-t px-4 py-3 flex items-center gap-2 shrink-0">
              <Button
                onClick={() => handleConfirm(true)}
                disabled={confirmLoading}
                className="flex-1"
              >
                {confirmLoading ? (
                  <Loader2 className="h-4 w-4 animate-spin mr-1" />
                ) : (
                  <Check className="h-4 w-4 mr-1" />
                )}
                Confirm & Next
              </Button>
              <Button
                variant="outline"
                onClick={() => handleConfirm(false)}
                disabled={confirmLoading}
              >
                Confirm
              </Button>
              <Button variant="ghost" onClick={skipToNext}>
                <SkipForward className="h-4 w-4 mr-1" /> Skip
              </Button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // -------------------------------------------------------------------------
  // Queue View
  // -------------------------------------------------------------------------

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/">
              <Button variant="ghost" size="sm">
                <ArrowLeft className="h-4 w-4 mr-1" /> Home
              </Button>
            </Link>
            <h1 className="text-xl font-semibold">Invoice Review Queue</h1>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex gap-2 text-xs">
              {errorCount > 0 && (
                <span className="px-2 py-1 bg-red-100 text-red-700 rounded-full">
                  {errorCount} failed
                </span>
              )}
              {processingCount > 0 && (
                <span className="px-2 py-1 bg-blue-100 text-blue-700 rounded-full">
                  {processingCount} processing
                </span>
              )}
              {extractedCount > 0 && (
                <span className="px-2 py-1 bg-yellow-100 text-yellow-700 rounded-full">
                  {extractedCount} to review
                </span>
              )}
              {confirmedCount > 0 && (
                <span className="px-2 py-1 bg-green-100 text-green-700 rounded-full">
                  {confirmedCount} confirmed
                </span>
              )}
            </div>
            {confirmedCount > 0 && (
              <>
                <Button variant="outline" size="sm" onClick={() => handleExport("xlsx")}>
                  <Download className="h-4 w-4 mr-1" /> Excel
                </Button>
                <Button variant="outline" size="sm" onClick={() => handleExport("csv")}>
                  <Download className="h-4 w-4 mr-1" /> CSV
                </Button>
              </>
            )}
            <Button variant="ghost" size="sm" onClick={() => setShowSettings(!showSettings)}>
              <Settings className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </header>

      <div className="max-w-6xl mx-auto px-6 py-6 space-y-6">
        {/* Settings panel */}
        {showSettings && <SettingsPanel onClose={() => setShowSettings(false)} />}

        {/* Rate limit warning */}
        {rateLimitError && (
          <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 flex items-start gap-3">
            <AlertTriangle className="h-5 w-5 text-red-500 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-medium text-red-800">Rate limit reached</p>
              <p className="text-xs text-red-600 mt-0.5">{rateLimitError}</p>
            </div>
            <button onClick={() => setRateLimitError(null)} className="ml-auto text-red-400 hover:text-red-600">
              <X className="h-4 w-4" />
            </button>
          </div>
        )}

        {/* Upload area */}
        <Card>
          <CardContent className="pt-6">
            <div
              onDragOver={(e) => e.preventDefault()}
              onDrop={handleDrop}
              onClick={handleFileSelect}
              className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center cursor-pointer hover:border-blue-400 hover:bg-blue-50/50 transition-colors"
            >
              <Upload className="h-8 w-8 text-gray-400 mx-auto mb-3" />
              <p className="text-sm font-medium text-gray-600">
                Drop invoice PDFs here or click to browse
              </p>
              <p className="text-xs text-gray-400 mt-1">
                Files will be uploaded, parsed, and queued for review automatically
              </p>
              {rateLimits && rateLimits.remaining_today !== null && (
                <p className={`text-xs mt-2 ${rateLimits.remaining_today <= 5 ? "text-orange-500 font-medium" : "text-gray-400"}`}>
                  {rateLimits.remaining_today} of {rateLimits.max_documents_per_day} daily documents remaining
                  {rateLimits.max_files_per_upload > 0 && ` · max ${rateLimits.max_files_per_upload} per upload`}
                </p>
              )}
              <input
                ref={fileInputRef}
                type="file"
                accept=".pdf"
                multiple
                className="hidden"
                onChange={handleFilesChosen}
              />
            </div>
          </CardContent>
        </Card>

        {/* Processing errors — compact banner */}
        {processingErrors.length > 0 && !showErrorsPanel && (
          <button
            onClick={() => setShowErrorsPanel(true)}
            className="w-full bg-red-50 border border-red-200 rounded-lg px-4 py-3 flex items-center gap-3 hover:bg-red-100 transition-colors text-left"
          >
            <AlertTriangle className="h-4 w-4 text-red-500 shrink-0" />
            <span className="text-sm font-medium text-red-800">
              {processingErrors.length} processing {processingErrors.length === 1 ? "failure" : "failures"}
            </span>
            <span className="text-xs text-red-500 ml-auto">View details</span>
          </button>
        )}

        {/* Queue table */}
        {invoices.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Invoices ({invoices.length})</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-left text-gray-500">
                      <th className="pb-2 font-medium">Filename</th>
                      <th className="pb-2 font-medium">Status</th>
                      <th className="pb-2 font-medium">Fields Extracted</th>
                      <th className="pb-2 font-medium text-right">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {invoices.map((inv, idx) => (
                      <tr
                        key={inv.file_path + idx}
                        className="border-b last:border-0 hover:bg-gray-50"
                      >
                        <td className="py-3 flex items-center gap-2">
                          <FileText className="h-4 w-4 text-gray-400 shrink-0" />
                          <span className="truncate max-w-xs">{inv.filename}</span>
                        </td>
                        <td className="py-3">
                          <StatusBadge status={inv.status} />
                        </td>
                        <td className="py-3 text-gray-500">
                          {(() => {
                            const fields = inv.confirmed_fields ?? inv.extracted_fields;
                            return fields
                              ? `${Object.values(fields).filter((v) => v !== null && v !== "").length}/${fieldConfig.length}`
                              : "-";
                          })()}
                        </td>
                        <td className="py-3 text-right">
                          {["extracted", "confirmed"].includes(inv.status) && (
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => openReview(idx)}
                            >
                              {inv.status === "confirmed" ? "View" : "Review"}
                            </Button>
                          )}
                          {inv.status === "error" && (
                            <span className="text-xs text-red-500 truncate max-w-xs inline-block">
                              {inv.error}
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        )}

        {invoices.length === 0 && (
          <div className="text-center py-12 text-gray-400">
            <FileText className="h-12 w-12 mx-auto mb-3 opacity-50" />
            <p>No invoices yet. Upload PDFs to get started.</p>
          </div>
        )}
      </div>

      {/* Errors slide-out panel */}
      {showErrorsPanel && (
        <div className="fixed inset-0 z-50 flex justify-end">
          <div className="absolute inset-0 bg-black/20" onClick={() => setShowErrorsPanel(false)} />
          <div className="relative w-full max-w-lg bg-white shadow-xl flex flex-col">
            <div className="px-4 py-3 border-b flex items-center justify-between shrink-0">
              <h2 className="text-sm font-semibold text-red-800 flex items-center gap-2">
                <AlertTriangle className="h-4 w-4" />
                Processing Failures ({processingErrors.length})
              </h2>
              <Button variant="ghost" size="sm" onClick={() => setShowErrorsPanel(false)}>
                <X className="h-4 w-4" />
              </Button>
            </div>
            <div className="flex-1 overflow-auto">
              {processingErrors.length === 0 ? (
                <div className="text-center py-12 text-gray-400 text-sm">No failures</div>
              ) : (
                <div className="divide-y">
                  {processingErrors.map((err, idx) => (
                    <div key={err.file_path + err.stage + idx} className="px-4 py-3 hover:bg-red-50/50">
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2">
                            <FileText className="h-4 w-4 text-red-400 shrink-0" />
                            <span className="text-sm font-medium truncate">{err.filename}</span>
                            <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-red-100 text-red-700 shrink-0">
                              {err.stage}
                            </span>
                          </div>
                          <p className="text-xs text-gray-500 mt-1 line-clamp-2" title={err.error_message}>
                            {err.error_message}
                          </p>
                          <p className="text-[10px] text-gray-400 mt-1">
                            {err.created_at ? new Date(err.created_at).toLocaleString() : ""}
                          </p>
                        </div>
                        <div className="flex items-center gap-1 shrink-0">
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={retryingPaths.has(err.file_path)}
                            onClick={() => handleRetry(err.file_path)}
                          >
                            {retryingPaths.has(err.file_path) ? (
                              <Loader2 className="h-3 w-3 animate-spin" />
                            ) : (
                              <RotateCcw className="h-3 w-3" />
                            )}
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDismissError(err.file_path, err.stage)}
                          >
                            <X className="h-3 w-3" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    uploading: "bg-blue-100 text-blue-700",
    parsing: "bg-blue-100 text-blue-700",
    extracting: "bg-blue-100 text-blue-700",
    extracted: "bg-yellow-100 text-yellow-700",
    confirmed: "bg-green-100 text-green-700",
    error: "bg-red-100 text-red-700",
  };

  const labels: Record<string, string> = {
    uploading: "Uploading...",
    parsing: "Parsing...",
    extracting: "Extracting fields...",
    extracted: "Ready for review",
    confirmed: "Confirmed",
    error: "Error",
  };

  const isProcessing = ["uploading", "parsing", "extracting"].includes(status);

  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${styles[status] || "bg-gray-100 text-gray-600"}`}>
      {isProcessing && <Loader2 className="h-3 w-3 animate-spin" />}
      {status === "confirmed" && <Check className="h-3 w-3" />}
      {labels[status] || status}
    </span>
  );
}

function SettingsPanel({ onClose }: { onClose: () => void }) {
  const [warehouseId, setWarehouseId] = useState("");
  const [volumePath, setVolumePath] = useState("");
  const [deltaTable, setDeltaTable] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    Promise.all([
      apiCall("/api/warehouse-config"),
      apiCall("/api/volume-path-config"),
      apiCall("/api/delta-table-path-config"),
    ]).then(([wh, vol, dt]) => {
      setWarehouseId(wh.warehouse_id || "");
      setVolumePath(vol.volume_path || "");
      setDeltaTable(dt.delta_table_path || "");
    });
  }, []);

  const save = async () => {
    setSaving(true);
    try {
      await Promise.all([
        apiCall("/api/warehouse-config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ warehouse_id: warehouseId }),
        }),
        apiCall("/api/volume-path-config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ volume_path: volumePath }),
        }),
        apiCall("/api/delta-table-path-config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ delta_table_path: deltaTable }),
        }),
      ]);
      onClose();
    } catch (e) {
      console.error("Settings save failed:", e);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">Settings</CardTitle>
          <Button variant="ghost" size="sm" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Warehouse ID</label>
            <input
              value={warehouseId}
              onChange={(e) => setWarehouseId(e.target.value)}
              className="w-full px-3 py-2 text-sm border rounded-md"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">UC Volume Path</label>
            <input
              value={volumePath}
              onChange={(e) => setVolumePath(e.target.value)}
              className="w-full px-3 py-2 text-sm border rounded-md"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Delta Table Path</label>
            <input
              value={deltaTable}
              onChange={(e) => setDeltaTable(e.target.value)}
              className="w-full px-3 py-2 text-sm border rounded-md"
            />
          </div>
        </div>
        <div className="mt-4 flex justify-end">
          <Button size="sm" onClick={save} disabled={saving}>
            {saving ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : null}
            Save Settings
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Load field config on mount + invoice queue
  useEffect(() => {
    apiCall("/api/invoice-fields").then((res) => {
      if (res.fields) setFieldConfig(res.fields);
    }).catch(console.error);

    loadQueue();
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

    for (const file of fileArray) {
      await processOneInvoice(file);
    }
  };

  const handleDrop = async (e: React.DragEvent) => {
    e.preventDefault();
    const files = Array.from(e.dataTransfer.files).filter(
      (f) => f.type === "application/pdf" || f.name.toLowerCase().endsWith(".pdf")
    );
    for (const file of files) {
      await processOneInvoice(file);
    }
  };

  const processOneInvoice = async (file: File) => {
    const tempPath = `__pending__/${file.name}`;
    const newInvoice: Invoice = {
      file_path: tempPath,
      filename: file.name,
      status: "uploading",
      extracted_fields: null,
      confirmed_fields: null,
      file,
    };

    setInvoices((prev) => [newInvoice, ...prev]);

    try {
      // Step 1: Upload
      const formData = new FormData();
      formData.append("files", file);
      const uploadRes = await apiCall("/api/upload-to-uc", { method: "POST", body: formData });
      const ucPath = uploadRes.uploaded_files?.[0]?.path;
      if (!ucPath) throw new Error("Upload failed — no path returned");

      setInvoices((prev) =>
        prev.map((inv) =>
          inv.file_path === tempPath
            ? { ...inv, file_path: ucPath, ucPath, status: "parsing" }
            : inv
        )
      );

      // Step 2: Parse (ai_parse_document) — fire and poll
      apiCall("/api/write-to-delta-table", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_paths: [ucPath] }),
      }).catch(() => {});

      // Poll until parsed
      let parsed = false;
      for (let attempt = 0; attempt < 60; attempt++) {
        await sleep(5000);
        try {
          const queryRes = await apiCall("/api/query-delta-table", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ file_paths: [ucPath] }),
          });
          if (queryRes.success && queryRes.data && queryRes.data.length > 0) {
            parsed = true;
            break;
          }
        } catch {
          // Keep polling
        }
      }

      if (!parsed) throw new Error("Document parsing timed out");

      setInvoices((prev) =>
        prev.map((inv) =>
          inv.file_path === ucPath ? { ...inv, status: "extracting" } : inv
        )
      );

      // Step 3: Extract fields (retry up to 3 times for timeouts)
      let extractRes = null;
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          extractRes = await apiCall("/api/extract-fields", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ file_path: ucPath }),
          });
          if (extractRes.success) break;
        } catch (e) {
          console.warn(`Extract attempt ${attempt + 1}/3 failed:`, e);
          if (attempt < 2) await sleep(5000 * (attempt + 1));
        }
      }

      if (extractRes?.success && extractRes.fields) {
        setInvoices((prev) =>
          prev.map((inv) =>
            inv.file_path === ucPath
              ? { ...inv, status: "extracted", extracted_fields: extractRes.fields }
              : inv
          )
        );
      } else {
        // Extraction failed but document is parsed — still let user review manually
        const errorDetail = extractRes?.error || "Field extraction failed after retries";
        console.error("Extract fields error:", errorDetail);
        setInvoices((prev) =>
          prev.map((inv) =>
            inv.file_path === ucPath
              ? {
                  ...inv,
                  status: "extracted",
                  extracted_fields: extractRes?.fields || {},
                  error: errorDetail,
                }
              : inv
          )
        );
      }

      // Refresh queue from server
      loadQueue();
    } catch (err: any) {
      const errMsg = err?.message || String(err);
      setInvoices((prev) =>
        prev.map((inv) =>
          inv.file_path === tempPath || inv.filename === file.name
            ? { ...inv, status: "error", error: errMsg }
            : inv
        )
      );
    }
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

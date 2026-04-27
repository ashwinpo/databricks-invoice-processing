/**
 * API configuration for Databricks Apps environment.
 * Handles base URL detection — always uses same-origin.
 */

export function getBaseUrl(): string {
  // In Databricks Apps, the API is served from the same origin.
  // Always prefer same-origin — the proxy routes to the backend automatically.
  if (typeof window !== "undefined") {
    return window.location.origin;
  }
  return "";
}

/**
 * Make an API call with automatic base URL resolution.
 *
 * Uses same-origin only — no fallback loop. The previous fallback strategy
 * caused file uploads to fail because FormData bodies can only be read once;
 * when the first URL failed, the retry sent an empty body.
 */
export async function apiCall(
  endpoint: string,
  options: RequestInit = {}
): Promise<any> {
  const baseUrl = getBaseUrl();
  const url = `${baseUrl}${endpoint}`;

  // Don't set Content-Type for FormData — browser sets it with boundary
  const headers: Record<string, string> = options.body instanceof FormData
    ? { ...options.headers as Record<string, string> }
    : { "Content-Type": "application/json", ...options.headers as Record<string, string> };

  const response = await fetch(url, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`API error (${response.status}): ${errorText}`);
  }

  return await response.json();
}

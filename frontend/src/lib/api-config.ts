/**
 * API configuration for Databricks Apps environment.
 * Handles base URL detection with fallback strategy.
 */

export function getBaseUrl(): string {
  // In Databricks Apps, the API is served from the same origin
  if (typeof window !== "undefined") {
    // Check for explicit env var first
    const envUrl = process.env.NEXT_PUBLIC_API_URL;
    if (envUrl) {
      return envUrl;
    }
    // Default: same origin (works in Databricks Apps)
    return window.location.origin;
  }
  return "";
}

/**
 * Make an API call with automatic base URL resolution.
 * Tries the primary URL first, falls back to alternatives.
 */
export async function apiCall(
  endpoint: string,
  options: RequestInit = {}
): Promise<any> {
  const baseUrl = getBaseUrl();
  const urls = [
    `${baseUrl}${endpoint}`,
    // Fallback: try without port (Databricks Apps proxies to backend)
    endpoint,
  ];

  let lastError: Error | null = null;

  for (const url of urls) {
    try {
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
    } catch (error) {
      lastError = error as Error;
      console.warn(`API call failed for ${url}:`, error);
      continue;
    }
  }

  throw lastError || new Error(`All API URLs failed for ${endpoint}`);
}

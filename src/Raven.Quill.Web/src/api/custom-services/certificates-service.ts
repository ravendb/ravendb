import { API_ENDPOINTS } from "@/api/generated/server-api";
import type { ApiErrorResponse, CertificateItem, DatabaseAccess, SecurityClearance } from "@/api/generated/server-api";
import type { ApiClient } from "@/api/http-client";

// The certificate GET response is typed as CertificateItem by the backend, so this service
// reuses that generated type. It still adds the wire shapes the schema can't express:
// certificatesGenerate responds with a zip download (typed as void).

export type { CertificateItem, SecurityClearance };

export function createCertificatesService(client: ApiClient) {
    return {
        list: (searchParams: { start: number; pageSize: number }) =>
            client.get<CertificateItem[], ApiErrorResponse>(API_ENDPOINTS.settings.certificates, { searchParams }),
        generate: (body: {
            name: string;
            clearance: SecurityClearance;
            password?: string;
            permissions: Record<string, DatabaseAccess>;
        }) =>
            client.post<Blob, ApiErrorResponse>(API_ENDPOINTS.settings.certificatesGenerate, body, {
                responseType: "blob",
            }),
    };
}

export type CertificatesService = ReturnType<typeof createCertificatesService>;

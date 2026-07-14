import { API_ENDPOINTS } from "@/api/generated/server-api";
import type { ApiErrorResponse, DatabaseAccess } from "@/api/generated/server-api";
import type { ApiClient } from "@/api/http-client";

// The OpenAPI contract types CertificateDefinition as an untyped record (the RavenDB
// client class is not introspectable by the schema generator), and the generated
// certificatesGenerate operation hides that the server responds with a zip file.
// This service adds the real wire shapes on top of the same endpoints.

export type SecurityClearance = "UnauthenticatedClients" | "ClusterAdmin" | "ClusterNode" | "Operator" | "ValidUser";

export type CertificateItem = {
    name: string | null;
    thumbprint: string;
    securityClearance: SecurityClearance;
    notAfter?: string | null;
    notBefore?: string | null;
    permissions?: Record<string, DatabaseAccess> | null;
    disabled?: boolean;
};

export function createCertificatesService(client: ApiClient) {
    return {
        list: (searchParams: { start: number; pageSize: number }) =>
            client.get<CertificateItem[], ApiErrorResponse>(API_ENDPOINTS.settings.certificates, { searchParams }),
        generate: (searchParams: { appName: string; name: string }) =>
            client.post<Blob, ApiErrorResponse>(API_ENDPOINTS.settings.certificatesGenerate, undefined, {
                responseType: "blob",
                searchParams,
            }),
        edit: (
            permissions: Record<string, DatabaseAccess>,
            searchParams: { thumbprint: string; name: string; disable: boolean },
        ) =>
            client.post<void, ApiErrorResponse>(API_ENDPOINTS.settings.certificatesEdit, permissions, {
                searchParams,
            }),
    };
}

export type CertificatesService = ReturnType<typeof createCertificatesService>;

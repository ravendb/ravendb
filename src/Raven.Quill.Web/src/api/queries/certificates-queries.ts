import { queryOptions } from "@tanstack/react-query";
import type { CertificatesService } from "@/api/custom-services/certificates-service";

const baseKey = "certificates";

// The view manages a handful of client certificates, so one page is plenty.
const CERTIFICATES_PAGE_SIZE = 1024;

export function createCertificatesQueries(api: CertificatesService) {
    return {
        list: () =>
            queryOptions({
                queryKey: [baseKey, "list"],
                queryFn: () => api.list({ start: 0, pageSize: CERTIFICATES_PAGE_SIZE }),
            }),
    };
}

export type CertificatesQueries = ReturnType<typeof createCertificatesQueries>;

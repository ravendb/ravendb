import { queryOptions } from "@tanstack/react-query";
import type { ApiClient } from "@/api/http-client";

export type BootstrapPhase = "needs-activation" | "redeeming" | "restarting" | "ready";

export type BootstrapStatus = {
    state: BootstrapPhase;
    reason?: string | null;
};

export type RedeemLicenseRequest = {
    licenseKey: string;
};

export type RedeemLicenseResponse = {
    state: BootstrapPhase;
};

export function createBootstrapService(client: ApiClient) {
    return {
        getStatus: () => client.get<BootstrapStatus>("/bootstrap/status"),
        redeemLicense: (request: RedeemLicenseRequest) =>
            client.post<RedeemLicenseResponse>("/bootstrap/redeem-license", request),
    };
}

export type BootstrapService = ReturnType<typeof createBootstrapService>;

export function createBootstrapQueries(api: BootstrapService) {
    return {
        status: () =>
            queryOptions({
                queryKey: ["bootstrap", "status"],
                queryFn: () => api.getStatus(),
            }),
    };
}

export type BootstrapQueries = ReturnType<typeof createBootstrapQueries>;

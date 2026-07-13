import type { LicenseResponse, QuillUsageResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const settingsMocks = {
    license: (response: LicenseResponse = sampleLicense) =>
        apiHttp.get("/api/settings/license", ({ response: res }) => res(200).json(response)),
    usage: (response: QuillUsageResponse = sampleQuillUsage) =>
        apiHttp.get("/api/settings/usage", ({ response: res }) => res(200).json(response)),
};

export const sampleLicense: LicenseResponse = {
    response: {
        errorMessage: "",
        expiration: "2026-07-14T00:00:00Z",
        subscriptionExpiration: "2026-07-14T00:00:00Z",
        expired: false,
        firstServerStartDate: "2026-06-14T00:00:00Z",
        id: "1e1ad2a9-02de-4e2b-a2f2-183ac9b1c04f",
        licensedTo: "Acme Corp",
        status: "Commercial",
        type: "Quill",
        version: "7.2",
    },
    connectivity: {
        statusCode: "OK",
        exception: "",
    },
    plans: [
        {
            slug: "enterprise",
            name: "Enterprise",
            tagline: "Production workloads",
            priceLabel: "Custom",
            priceSuffix: "",
            featured: false,
            features: ["Unlimited apps & writes", "2h SLA support"],
        },
    ],
};

// ~30 daily points with a gentle wave so the writes chart has shape.
export const sampleQuillUsage: QuillUsageResponse = {
    byPeriod: Array.from({ length: 30 }, (_, index) => {
        const day = String(index + 1).padStart(2, "0");
        const wave = Math.sin((index / 29) * Math.PI * 2);
        return {
            from: `2026-06-${day}T00:00:00Z`,
            to: `2026-06-${day}T23:59:59Z`,
            usage: Math.round(280000 + wave * 130000),
        };
    }),
    perApplication: [
        {
            topologyId: "topology-1",
            applicationName: "support-copilot",
            from: "2026-06-01T00:00:00Z",
            to: "2026-06-30T23:59:59Z",
            usage: 5200000,
        },
        {
            topologyId: "topology-1",
            applicationName: "orders-sync",
            from: "2026-06-01T00:00:00Z",
            to: "2026-06-30T23:59:59Z",
            usage: 2400000,
        },
        {
            topologyId: "topology-2",
            applicationName: "docs-assistant",
            from: "2026-06-01T00:00:00Z",
            to: "2026-06-30T23:59:59Z",
            usage: 1300000,
        },
    ],
};

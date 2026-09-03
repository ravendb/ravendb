import { http, HttpResponse } from "msw";
import type { LicenseResponse, QuillApplicationUsage, QuillUsageResponse } from "@/api/generated/server-api";
import type { CertificateItem } from "@/api/custom-services/certificates-service";
import { apiHttp } from "./api-http";
import { MS_IN } from "@/lib/time";

export const settingsMocks = {
    feedback: () => apiHttp.post("/api/settings/feedback", ({ response }) => response(204).empty()),
    license: (response: LicenseResponse = sampleLicense) =>
        apiHttp.get("/api/settings/license", ({ response: res }) => res(200).json(response)),
    usage: (response: QuillUsageResponse = sampleQuillUsage) =>
        apiHttp.get("/api/settings/usage", ({ response: res }) => res(200).json(response)),
    certificates: (response: CertificateItem[] = sampleCertificates) =>
        apiHttp.get("/api/settings/certificates/get", ({ response: res }) => res(200).json(response)),
    // The OpenAPI contract only documents the 400 responses for generate (real
    // success is a zip download) and edit (empty 200), so these use plain msw.
    certificatesGenerate: () =>
        http.post(
            "/api/settings/certificates/generate",
            () => new HttpResponse("mock certificate zip", { headers: { "Content-Type": "application/octet-stream" } }),
        ),
    certificatesGenerateError: (message = "A certificate with this name already exists.") =>
        http.post("/api/settings/certificates/generate", () => HttpResponse.json({ error: message }, { status: 400 })),
    certificatesEdit: () => http.post("/api/settings/certificates/edit", () => new HttpResponse(null, { status: 200 })),
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

// Databases match sampleApps (demo-shop, support-desk) so the certificates view
// resolves app names for permission entries.
export const sampleCertificates: CertificateItem[] = [
    {
        name: "Server Certificate",
        thumbprint: "1E45C9F8A3B27D604B1347D2E859A0C4F6B8D2E1",
        securityClearance: "ClusterNode",
        notBefore: "2025-11-02T00:00:00Z",
        notAfter: "2027-11-02T00:00:00Z",
        permissions: {},
        disabled: false,
    },
    {
        name: "quill-backend",
        thumbprint: "7A2F0B7C4E91D8356F20A1B9C83D45E6F7180C2D",
        securityClearance: "ValidUser",
        notBefore: "2026-01-10T00:00:00Z",
        notAfter: "2031-01-10T00:00:00Z",
        permissions: { "demo-shop": "Admin", "support-desk": "Admin" },
        disabled: false,
    },
    {
        name: "reporting",
        thumbprint: "C90D1E2F3A4B5C6D7E8F9012A3B4C5D6E7F80913",
        securityClearance: "ValidUser",
        notBefore: "2025-06-20T00:00:00Z",
        notAfter: "2026-06-20T00:00:00Z",
        permissions: { "demo-shop": "ReadWrite" },
        disabled: false,
    },
    {
        name: "mobile-gateway",
        thumbprint: "3F8A9B0C1D2E4F5061728394A5B6C7D8E9F00112",
        securityClearance: "ValidUser",
        notBefore: "2025-08-01T00:00:00Z",
        // Expires one week from now so the "About to expire" badge always renders.
        notAfter: new Date(Date.now() + MS_IN.week).toISOString(),
        permissions: { "demo-shop": "ReadWrite" },
        disabled: false,
    },
    {
        name: "legacy-sync",
        thumbprint: "5B6C7D8E9F0A1B2C3D4E5F60718293A4B5C6D7E8",
        securityClearance: "ValidUser",
        notBefore: "2026-02-01T00:00:00Z",
        notAfter: "2028-02-01T00:00:00Z",
        permissions: { "support-desk": "ReadWrite" },
        disabled: true,
    },
];

// The shape RavenDB reports: unpadded base64 of a 16-byte guid - 22 characters over A-Za-z0-9+/,
// always ending in one of the four the final two bits allow. A couple carry a + or a /, because ids
// that awkward to read are exactly why the table groups by name and only falls back to them.
export const sampleTopologyIds = {
    systemBusiest: "j9T7K/m7IZsA7MorB0UVmw",
    system: "dFGqCGUYc8DUeweTsCxudA",
    systemQuietest: "26jT6mFmbaClQn7J0vE1Yg",
    huetopiaBusiest: "NA+WUHfUc3cvjURuPbGDFQ",
    huetopia: "C3i3GQcS1dgeNvaAYmIHDQ",
    supportCopilot: "Y44Uy3cyOPO0UdiBXNPCeg",
    ordersSync: "5xTOTLh5XkChGKDVQRCK5Q",
    bookshopHelper: "2ODd5OiMFOwoH1TF1cOhJQ",
};

function usageRow(
    topologyId: string,
    applicationName: string,
    usage: number,
    { isSystem = false }: { isSystem?: boolean } = {},
): QuillApplicationUsage {
    return {
        topologyId,
        applicationName,
        from: "2026-06-01T00:00:00Z",
        to: "2026-06-30T23:59:59Z",
        usage,
        isSystem,
    };
}

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
    // Mirrors what a real licence covering several appliances reports: many system rows sharing the
    // config database's name, a repeated app name, and unique apps - each distinguished only by
    // topology id, and deliberately out of order so the grouping is doing the work.
    perApplication: [
        usageRow(sampleTopologyIds.system, "quill-config", 2600, { isSystem: true }),
        usageRow(sampleTopologyIds.supportCopilot, "support-copilot", 5200000),
        usageRow(sampleTopologyIds.huetopiaBusiest, "huetopia", 1300000),
        usageRow(sampleTopologyIds.systemQuietest, "quill-config", 900, { isSystem: true }),
        usageRow(sampleTopologyIds.ordersSync, "orders-sync", 2400000),
        usageRow(sampleTopologyIds.systemBusiest, "quill-config", 4100, { isSystem: true }),
        usageRow(sampleTopologyIds.huetopia, "huetopia", 640000),
        usageRow(sampleTopologyIds.bookshopHelper, "bookshop-helper", 88000),
    ],
};

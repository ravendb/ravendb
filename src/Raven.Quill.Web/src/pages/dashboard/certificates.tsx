import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ExternalLink, Plus, RefreshCw } from "lucide-react";
import { api } from "@/api/api";
import type { CertificateItem, SecurityClearance } from "@/api/custom-services/certificates-service";
import type { AppResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { CardListSkeleton } from "@/components/data/loading-skeletons";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { CertificateCard } from "@/pages/dashboard/certificates/certificate-card";
import {
    getCertificateState,
    isEditableCertificate,
    type CertificateState,
} from "@/pages/dashboard/certificates/certificate-labels";
import { CertificatesToolbar, type CertificateSort } from "@/pages/dashboard/certificates/certificates-toolbar";
import { GenerateCertificateDialog } from "@/pages/dashboard/certificates/generate-certificate-dialog";
import { originForSubdomain } from "@/lib/subdomain-origin";

interface CertificateFilters {
    search: string;
    clearance: SecurityClearance | "all";
    state: CertificateState | "all";
}

export function DashboardCertificates() {
    const certificatesQuery = useQuery(api.queries.certificates.list());
    const appsQuery = useQuery(api.queries.apps.list());
    const apps = appsQuery.data ?? [];

    const [search, setSearch] = useState("");
    const [clearance, setClearance] = useState<SecurityClearance | "all">("all");
    const [state, setState] = useState<CertificateState | "all">("all");
    const [sort, setSort] = useState<CertificateSort>("name-asc");

    const certificates = certificatesQuery.data ?? [];
    const visibleCertificates = certificates
        .filter((certificate) => matchesFilters(certificate, { search, clearance, state }))
        .sort((a, b) => compareCertificates(a, b, sort));

    // Cluster-level certificates are managed by the server; Quill manages the rest.
    const serverCertificates = visibleCertificates.filter((certificate) => !isEditableCertificate(certificate));
    const clientCertificates = visibleCertificates.filter(isEditableCertificate);

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between gap-3">
                <div>
                    <h1 className="text-2xl font-semibold tracking-tight">Certificates</h1>
                </div>
                <div className="flex items-center gap-2">
                    <Button variant="outline" size="sm" asChild>
                        <a href={originForSubdomain("db")} target="_blank" rel="noreferrer">
                            <ExternalLink aria-hidden="true" />
                            Open database
                        </a>
                    </Button>
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={() => certificatesQuery.refetch()}
                        disabled={certificatesQuery.isFetching}
                    >
                        <RefreshCw aria-hidden="true" />
                        Refresh
                    </Button>
                    <GenerateCertificateDialog
                        apps={apps}
                        trigger={
                            <Button size="sm">
                                <Plus aria-hidden="true" />
                                Generate client certificate
                            </Button>
                        }
                    />
                </div>
            </div>

            <CertificatesToolbar
                search={search}
                onSearchChange={setSearch}
                clearance={clearance}
                onClearanceChange={setClearance}
                state={state}
                onStateChange={setState}
                sort={sort}
                onSortChange={setSort}
            />

            <ApiState
                isLoading={certificatesQuery.isPending}
                isError={certificatesQuery.isError}
                errorTitle="Could not load certificates"
                onRetry={() => certificatesQuery.refetch()}
                loadingLabel="Loading certificates…"
                skeleton={<CardListSkeleton />}
            >
                {visibleCertificates.length === 0 ? (
                    <div className="rounded-lg border p-8 text-center text-sm text-muted-foreground">
                        {certificates.length === 0
                            ? "No certificates yet."
                            : "No certificates match the current filters."}
                    </div>
                ) : (
                    <div className="space-y-8">
                        {serverCertificates.length > 0 && (
                            <CertificateSection title="Server" certificates={serverCertificates} apps={apps} />
                        )}
                        {clientCertificates.length > 0 && (
                            <CertificateSection title="Client" certificates={clientCertificates} apps={apps} />
                        )}
                    </div>
                )}
            </ApiState>
        </div>
    );
}

function CertificateSection({
    title,
    certificates,
    apps,
}: {
    title: string;
    certificates: CertificateItem[];
    apps: AppResponse[];
}) {
    return (
        <section className="space-y-3">
            <div className="flex items-center gap-2">
                <h2 className="text-sm font-semibold">{title}</h2>
                <Badge variant="secondary">{certificates.length}</Badge>
            </div>
            <div className="space-y-3">
                {certificates.map((certificate) => (
                    <CertificateCard key={certificate.thumbprint} certificate={certificate} apps={apps} />
                ))}
            </div>
        </section>
    );
}

function matchesFilters(certificate: CertificateItem, filters: CertificateFilters): boolean {
    const query = filters.search.trim().toLowerCase();
    if (
        query &&
        !certificate.name.toLowerCase().includes(query) &&
        !certificate.thumbprint.toLowerCase().includes(query)
    ) {
        return false;
    }
    if (filters.clearance !== "all" && certificate.securityClearance !== filters.clearance) {
        return false;
    }
    if (filters.state !== "all" && getCertificateState(certificate) !== filters.state) {
        return false;
    }
    return true;
}

function compareCertificates(a: CertificateItem, b: CertificateItem, sort: CertificateSort): number {
    switch (sort) {
        case "name-asc":
            return compareByName(a, b);
        case "name-desc":
            return compareByName(b, a);
        case "expiration-asc":
            return expirationTime(a) - expirationTime(b);
        case "expiration-desc":
            return expirationTime(b) - expirationTime(a);
    }
}

function compareByName(a: CertificateItem, b: CertificateItem): number {
    return a.name.localeCompare(b.name, undefined, { sensitivity: "base" }) || a.thumbprint.localeCompare(b.thumbprint);
}

// Certificates without an expiration sort as if they never expire.
function expirationTime(certificate: CertificateItem): number {
    return certificate.notAfter ? new Date(certificate.notAfter).getTime() : Number.MAX_SAFE_INTEGER;
}

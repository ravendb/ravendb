import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ExternalLink, Plus, RefreshCw } from "lucide-react";
import { api } from "@/api/api";
import type { CertificateItem, SecurityClearance } from "@/api/custom-services/certificates-service";
import type { AppResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { CardListSkeleton } from "@/components/data/loading-skeletons";
import { CountBadge } from "@/components/data/count-badge";
import { Button } from "@/components/shadcn/ui/button";
import {
    Tooltip,
    TooltipContent,
    TooltipProvider,
    TooltipTrigger,
} from "@/components/shadcn/ui/tooltip";
import { CertificateCard } from "@/pages/dashboard/certificates/certificate-card";
import {
    getCertificateState,
    isEditableCertificate,
    type CertificateState,
} from "@/pages/dashboard/certificates/certificate-labels";
import { CertificatesToolbar, type CertificateSort } from "@/pages/dashboard/certificates/certificates-toolbar";
import { GenerateCertificateDialog } from "@/pages/dashboard/certificates/generate-certificate-dialog";
import { CERTIFICATES_DOCS_URL } from "@/lib/help-links";
import { originForSubdomain } from "@/lib/subdomain-origin";
import { Heading, Text } from "@/components/typography";


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
            <div className="flex items-start justify-between gap-3">
                <div className="min-w-0 space-y-1">
                    <Heading as="h1" variant="page">
                        Certificates
                    </Heading>
                    <Text variant="muted" className="max-w-2xl">
                        Client certificates provide access to RavenDB databases associated with Quill apps, either
                        from applications using the RavenDB Client API or from RavenDB Studio in a browser.
                        <span className="mt-1 block">
                            <a
                                href={CERTIFICATES_DOCS_URL}
                                target="_blank"
                                rel="noreferrer"
                                className="underline underline-offset-4 hover:text-primary-strong"
                            >
                                Learn more about client certificates
                            </a>
                            .
                        </span>
                    </Text>
                </div>
                <div className="flex items-center gap-2">
                    <TooltipProvider>
                        <Tooltip>
                            <TooltipTrigger asChild>
                                <Button variant="outline" size="sm" asChild>
                                    <a href={originForSubdomain("db")} target="_blank" rel="noreferrer">
                                        <ExternalLink aria-hidden="true" />
                                        Open database
                                    </a>
                                </Button>
                            </TooltipTrigger>
                            <TooltipContent>
                                Open RavenDB Studio in a new tab to access the RavenDB databases associated with your
                                Quill apps. Your browser must have a client certificate installed.
                            </TooltipContent>
                        </Tooltip>
                    </TooltipProvider>
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
                    <Text as="div" variant="muted" className="rounded-lg border p-8 text-center">
                        {certificates.length === 0
                            ? "No client certificates yet. Generate one to connect an application to an app database or access RavenDB Studio."
                            : "No certificates match the current filters."}
                    </Text>
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
                <Heading variant="label">{title}</Heading>
                <CountBadge>{certificates.length}</CountBadge>
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

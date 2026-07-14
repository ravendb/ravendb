import { useQuery } from "@tanstack/react-query";
import { Copy, Plus, RefreshCw } from "lucide-react";
import { api } from "@/api/api";
import type { CertificateItem } from "@/api/custom-services/certificates-service";
import type { AppResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatDate } from "@/lib/format";
import { cn, copyToClipboard } from "@/lib/utils";
import { SectionTable } from "@/pages/apps/section-card";
import {
    DATABASE_ACCESS_LABELS,
    SECURITY_CLEARANCE_LABELS,
    isEditableCertificate,
    isExpiredCertificate,
} from "@/pages/dashboard/certificates/certificate-labels";
import { EditCertificateDialog } from "@/pages/dashboard/certificates/edit-certificate-dialog";
import { GenerateCertificateDialog } from "@/pages/dashboard/certificates/generate-certificate-dialog";

export function DashboardCertificates() {
    const certificatesQuery = useQuery(api.queries.certificates.list());
    const appsQuery = useQuery(api.queries.apps.list());
    const apps = appsQuery.data ?? [];

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between gap-3">
                <div>
                    <h1 className="text-2xl font-semibold tracking-tight">Certificates</h1>
                    <p className="text-sm text-muted-foreground">
                        Client certificates used to authenticate against the underlying RavenDB server.
                    </p>
                </div>
                <div className="flex items-center gap-2">
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

            <ApiState
                isLoading={certificatesQuery.isPending}
                isError={certificatesQuery.isError}
                errorTitle="Could not load certificates"
                onRetry={() => certificatesQuery.refetch()}
                loadingLabel="Loading certificates…"
            >
                {certificatesQuery.data && (
                    <SectionTable
                        headers={["Name", "Status", "Clearance", "App access", "Expiration", "Thumbprint", ""]}
                        isEmpty={certificatesQuery.data.length === 0}
                        emptyMessage="No certificates yet."
                    >
                        {certificatesQuery.data.map((certificate) => (
                            <CertificateRow key={certificate.thumbprint} certificate={certificate} apps={apps} />
                        ))}
                    </SectionTable>
                )}
            </ApiState>
        </div>
    );
}

function CertificateRow({ certificate, apps }: { certificate: CertificateItem; apps: AppResponse[] }) {
    const isExpired = isExpiredCertificate(certificate);

    return (
        <TableRow>
            <TableCell className="font-medium">{certificate.name || "—"}</TableCell>
            <TableCell>
                <CertificateStatusBadge isDisabled={Boolean(certificate.disabled)} isExpired={isExpired} />
            </TableCell>
            <TableCell>
                <Badge variant="outline">{SECURITY_CLEARANCE_LABELS[certificate.securityClearance]}</Badge>
            </TableCell>
            <TableCell>
                <CertificateAccess certificate={certificate} />
            </TableCell>
            <TableCell className={cn("whitespace-nowrap", isExpired ? "text-destructive" : "text-muted-foreground")}>
                {certificate.notAfter ? formatDate(certificate.notAfter) : "—"}
            </TableCell>
            <TableCell>
                <div className="flex items-center gap-1">
                    <span className="inline-block max-w-32 truncate font-mono text-xs text-muted-foreground">
                        {certificate.thumbprint}
                    </span>
                    <Button
                        type="button"
                        variant="ghost"
                        size="icon-sm"
                        aria-label="Copy thumbprint"
                        onClick={() => copyToClipboard(certificate.thumbprint)}
                    >
                        <Copy className="size-3.5" aria-hidden="true" />
                    </Button>
                </div>
            </TableCell>
            <TableCell className="text-right">
                {isEditableCertificate(certificate) && (
                    <EditCertificateDialog
                        certificate={certificate}
                        apps={apps}
                        trigger={
                            <Button variant="outline" size="sm">
                                Edit
                            </Button>
                        }
                    />
                )}
            </TableCell>
        </TableRow>
    );
}

function CertificateStatusBadge({ isDisabled, isExpired }: { isDisabled: boolean; isExpired: boolean }) {
    if (isDisabled) {
        return <Badge variant="secondary">Disabled</Badge>;
    }
    if (isExpired) {
        return <Badge variant="destructive">Expired</Badge>;
    }
    return <Badge variant="success">Valid</Badge>;
}

function CertificateAccess({ certificate }: { certificate: CertificateItem }) {
    if (certificate.securityClearance !== "ValidUser") {
        return <span className="text-muted-foreground">All apps</span>;
    }

    const permissions = Object.entries(certificate.permissions ?? {});
    if (permissions.length === 0) {
        return <span className="text-muted-foreground">None</span>;
    }

    return (
        <div className="flex flex-wrap gap-1">
            {permissions.map(([database, access]) => (
                <Badge key={database} variant="secondary" className="font-normal">
                    {database} · {DATABASE_ACCESS_LABELS[access]}
                </Badge>
            ))}
        </div>
    );
}

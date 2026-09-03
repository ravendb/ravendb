import type { ReactNode } from "react";
import { Copy } from "lucide-react";
import type { CertificateItem } from "@/api/custom-services/certificates-service";
import type { AppResponse } from "@/api/generated/server-api";
import { StatusIndicator, type StatusTone } from "@/components/data/status-indicator";
import { Timestamp } from "@/components/data/timestamp";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Heading, Text } from "@/components/typography";
import { cn, copyToClipboard } from "@/lib/utils";
import {
    CERTIFICATE_STATE_LABELS,
    DATABASE_ACCESS_LABELS,
    SECURITY_CLEARANCE_LABELS,
    getCertificateState,
    isAboutToExpireCertificate,
    isEditableCertificate,
    type CertificateState,
} from "@/pages/dashboard/certificates/certificate-labels";
import { EditCertificateDialog } from "@/pages/dashboard/certificates/edit-certificate-dialog";

const STATE_STRIP_CLASSES: Record<CertificateState, string> = {
    valid: "bg-success",
    expired: "bg-destructive",
    disabled: "bg-muted-foreground/60",
};

const STATE_TONES: Record<CertificateState, StatusTone> = {
    valid: "positive",
    expired: "danger",
    disabled: "muted",
};

export function CertificateCard({ certificate, apps }: { certificate: CertificateItem; apps: AppResponse[] }) {
    const state = getCertificateState(certificate);
    const isAboutToExpire = state === "valid" && isAboutToExpireCertificate(certificate);

    return (
        <div className="flex overflow-hidden rounded-lg border bg-card text-card-foreground">
            <div className={cn("w-1 shrink-0", STATE_STRIP_CLASSES[state])} aria-hidden="true" />
            <div className="min-w-0 flex-1 space-y-4 p-4">
                <div className="flex flex-wrap items-start justify-between gap-2">
                    <div className="min-w-0 space-y-1">
                        <div className="flex flex-wrap items-center gap-2">
                            <Heading as="h3" variant="label">
                                {certificate.name || "—"}
                            </Heading>
                            <StatusIndicator tone={STATE_TONES[state]} label={CERTIFICATE_STATE_LABELS[state]} />
                            {isAboutToExpire && <StatusIndicator tone="warning" label="About to expire" />}
                        </div>
                        <div className="flex items-center gap-1">
                            <Text as="span" variant="caption" className="truncate font-mono">
                                {certificate.thumbprint}
                            </Text>
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
                    </div>
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
                </div>

                <dl className="grid gap-x-8 gap-y-3 sm:grid-cols-2 lg:grid-cols-4">
                    <CertificateField label="Security clearance">
                        {SECURITY_CLEARANCE_LABELS[certificate.securityClearance]}
                    </CertificateField>
                    <CertificateField label="Valid from">
                        <Timestamp value={certificate.notBefore} dateVariant="short" textVariant="inherit" />
                    </CertificateField>
                    <CertificateField label="Expiration">
                        <span
                            className={cn(state === "expired" && "text-destructive", isAboutToExpire && "text-warning")}
                        >
                            <Timestamp value={certificate.notAfter} dateVariant="short" textVariant="inherit" />
                        </span>
                    </CertificateField>
                    <CertificateField label="App access">
                        <CertificateAccess certificate={certificate} />
                    </CertificateField>
                </dl>
            </div>
        </div>
    );
}

function CertificateField({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div className="space-y-1">
            <dt className="text-xs font-medium tracking-wide text-muted-foreground uppercase">{label}</dt>
            <dd className="text-sm">{children}</dd>
        </div>
    );
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

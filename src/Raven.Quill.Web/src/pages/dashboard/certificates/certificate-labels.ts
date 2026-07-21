import type { CertificateItem, SecurityClearance } from "@/api/custom-services/certificates-service";
import type { AppResponse, DatabaseAccess } from "@/api/generated/server-api";
import type { FormSelectOption } from "@/components/form/form-select";

export const DATABASE_ACCESS_LABELS: Record<DatabaseAccess, string> = {
    Admin: "Admin",
    ReadWrite: "Read/Write",
    Read: "Read",
};

export const DATABASE_ACCESS_OPTIONS: readonly FormSelectOption<DatabaseAccess>[] = [
    { value: "Admin", label: DATABASE_ACCESS_LABELS.Admin },
    { value: "ReadWrite", label: DATABASE_ACCESS_LABELS.ReadWrite },
    { value: "Read", label: DATABASE_ACCESS_LABELS.Read },
];

export const SECURITY_CLEARANCE_LABELS: Record<SecurityClearance, string> = {
    UnauthenticatedClients: "Unauthenticated",
    ClusterAdmin: "Cluster Admin",
    ClusterNode: "Cluster Node",
    Operator: "Operator",
    ValidUser: "User",
};

// Clearances Quill can assign to client certificates; cluster-level clearances
// are managed by the server itself.
export const CLEARANCE_OPTIONS: readonly FormSelectOption<"Operator" | "ValidUser">[] = [
    { value: "Operator", label: SECURITY_CLEARANCE_LABELS.Operator },
    { value: "ValidUser", label: SECURITY_CLEARANCE_LABELS.ValidUser },
];

// Permissions are keyed by RavenDB database name; show the app it belongs to when known.
export function toDatabaseOption(database: string, apps: AppResponse[]): FormSelectOption<string> {
    const app = apps.find((candidate) => candidate.database === database);
    return { value: database, label: app ? app.name : database };
}

export function isExpiredCertificate(certificate: CertificateItem, now: number = Date.now()): boolean {
    return certificate.notAfter != null && new Date(certificate.notAfter).getTime() < now;
}

// Quill manages Operator and ValidUser certificates; cluster-level clearances
// (ClusterAdmin, ClusterNode) stay read-only here.
export function isEditableCertificate(certificate: CertificateItem): boolean {
    return certificate.securityClearance === "Operator" || certificate.securityClearance === "ValidUser";
}

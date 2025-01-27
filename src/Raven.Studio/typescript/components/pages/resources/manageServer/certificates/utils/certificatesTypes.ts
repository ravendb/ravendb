import { unitOfTime } from "moment";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
type DatabaseAccess = Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess;

export type CertificatesClearance = "Admin" | "Operator" | "User";

export type CertificatesState = "Valid" | "About to expire" | "Expired";

export type CertificatesSortMode =
    | "Default"
    | "By Name - Asc"
    | "By Name - Desc"
    | "By Expiration Date - Asc"
    | "By Expiration Date - Desc"
    | "By Valid-From Date - Asc"
    | "By Valid-From Date - Desc"
    | "By Last Used Date - Asc"
    | "By Last Used Date - Desc";

export interface CertificateItem extends CertificateDto {
    Thumbprints?: string[];
    LastUsedDate?: string;
}

export interface GenerateCertificateDto {
    Name: string;
    Password: string;
    SecurityClearance: SecurityClearance;
    NotAfter?: string;
    Permissions: Record<string, DatabaseAccess>;
    TwoFactorAuthenticationKey: string;
}

export interface UpdateCertificateDto {
    Name: string;
    Thumbprint: string;
    SecurityClearance: SecurityClearance;
    NotAfter?: string;
    Permissions: Record<string, DatabaseAccess>;
    TwoFactorAuthenticationKey: string;
}

export interface UploadCertificateDto {
    Name: string;
    Certificate: string;
    Password: string;
    Permissions: Record<string, DatabaseAccess>;
    SecurityClearance: SecurityClearance;
    NotAfter: string;
    TwoFactorAuthenticationKey: string;
}

export interface ReplaceServerCertificateDto {
    Certificate: string;
    Password: string;
}

export type ExpireTimeUnit = Extract<unitOfTime.Base, "days" | "months">;

export type TwoFactorAction = "leave" | "update" | "delete";

import { unitOfTime } from "moment";

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
    SecurityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
    NotAfter?: string;
    Permissions: Record<string, Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess>;
    TwoFactorAuthenticationKey: string;
}

export interface UpdateCertificateDto {
    Name: string;
    Thumbprint: string;
    SecurityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
    NotAfter?: string;
    Permissions: Record<string, Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess>;
    TwoFactorAuthenticationKey: string;
}

export type ExpireTimeUnit = Extract<unitOfTime.Base, "days" | "months">;

export type TwoFactorAction = "leave" | "update" | "delete";

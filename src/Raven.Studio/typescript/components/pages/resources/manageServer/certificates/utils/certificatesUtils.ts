import serverSettings from "common/settings/serverSettings";
import { ConfirmOptions } from "components/common/ConfirmDialog";
import { TextColor } from "components/models/common";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { CertificatesRegenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesRegenerateModal";
import {
    CertificateItem,
    CertificatesClearance,
    CertificatesState,
    ExpireTimeUnit,
    GenerateCertificateDto,
    TwoFactorAction,
    UpdateCertificateDto,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";
import * as yup from "yup";

type DatabaseAccess = Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess;

function getClearance(
    securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance
): CertificatesClearance {
    switch (securityClearance) {
        case "ClusterAdmin":
        case "ClusterNode":
            return "Admin";
        case "Operator":
            return "Operator";
        case "ValidUser":
            return "User";
        default:
            return null;
    }
}

function getState(notAfter: string): CertificatesState {
    const expirationDate = moment.utc(notAfter);
    const nowPlusExpirationThreshold = moment
        .utc()
        .add(serverSettings.default.certificateExpiringThresholdInDays(), "days");

    if (expirationDate.isBefore()) {
        return "Expired";
    }
    if (expirationDate.isBefore(nowPlusExpirationThreshold)) {
        return "About to expire";
    }
    return "Valid";
}

function getStateDateColor(state: CertificatesState): TextColor {
    switch (state) {
        case "Expired":
            return "danger";
        case "About to expire":
            return "warning";
        case "Valid":
            return null;
        default:
            return assertUnreachable(state);
    }
}

function getStateColor(state: CertificatesState): TextColor {
    switch (state) {
        case "Expired":
            return "danger";
        case "About to expire":
            return "warning";
        case "Valid":
            return "success";
        default:
            return assertUnreachable(state);
    }
}

function mapPermissions(
    formData: Pick<CertificatesGenerateFormData | CertificatesEditFormData, "securityClearance" | "databasePermissions">
): Record<string, DatabaseAccess> {
    if (formData.securityClearance === "ClusterAdmin" || formData.securityClearance === "Operator") {
        return null;
    }

    return Object.fromEntries(
        formData.databasePermissions.map(({ databaseName, accessLevel }) => [databaseName, accessLevel])
    );
}

function mapNotAfter(
    formData: Pick<CertificatesGenerateFormData | CertificatesRegenerateFormData, "expireIn" | "expireTimeUnits">
): string {
    if (!formData.expireIn) {
        return null;
    }

    return moment.utc().add(formData.expireIn, formData.expireTimeUnits).format();
}

function mapGenerateToDto(formData: CertificatesGenerateFormData): GenerateCertificateDto {
    return {
        Name: formData.name,
        Password: formData.certificatePassphrase,
        Permissions: mapPermissions({
            securityClearance: formData.securityClearance,
            databasePermissions: formData.databasePermissions,
        }),
        SecurityClearance: formData.securityClearance,
        NotAfter: mapNotAfter({
            expireIn: formData.expireIn,
            expireTimeUnits: formData.expireTimeUnits,
        }),
        TwoFactorAuthenticationKey: formData.isRequire2FA ? formData.authenticationKey : null,
    };
}

function mapEditToDto(formData: CertificatesEditFormData, certificate: CertificateItem): UpdateCertificateDto {
    return {
        Name: certificate.Name,
        Thumbprint: certificate.Thumbprint,
        SecurityClearance: formData.securityClearance,
        Permissions: mapPermissions({
            securityClearance: formData.securityClearance,
            databasePermissions: formData.databasePermissions,
        }),
        TwoFactorAuthenticationKey: formData.isRequire2FA ? formData.authenticationKey : null,
    };
}

function mapRegenerateToDto(
    formData: CertificatesRegenerateFormData,
    certificate: CertificateItem
): UpdateCertificateDto {
    return {
        Name: certificate.Name,
        Thumbprint: certificate.Thumbprint,
        SecurityClearance: certificate.SecurityClearance,
        Permissions: certificate.Permissions,
        NotAfter: mapNotAfter({
            expireIn: formData.expireIn,
            expireTimeUnits: formData.expireTimeUnits,
        }),
        TwoFactorAuthenticationKey: formData.isRequire2FA ? formData.authenticationKey : null,
    };
}

function mapDatabasePermissionsFromDto(dto: CertificateItem) {
    return Object.entries(dto.Permissions ?? []).map(([databaseName, accessLevel]) => ({
        databaseName,
        accessLevel,
    }));
}

const twoFactorActionSchema = yup
    .string<TwoFactorAction>()
    .nullable()
    .when("$certHasTwoFactor", {
        is: true,
        then: (schema) => schema.required(),
    });

const databasePermissionsSchema = yup
    .array()
    .of(yup.object({ databaseName: yup.string(), accessLevel: yup.string<DatabaseAccess>() }));

const noPrivilegesConfirmOptions: ConfirmOptions = {
    title: "Did you forget about assigning database privileges?",
    message: "Leaving the database privileges section empty is going to prevent users from accessing the database.",
    confirmText: "Save anyway",
};

export const certificatesUtils = {
    getClearance,
    getState,
    getStateColor,
    getStateDateColor,
    mapGenerateToDto,
    mapEditToDto,
    mapRegenerateToDto,
    mapDatabasePermissionsFromDto,
    twoFactorActionSchema,
    databasePermissionsSchema,
    noPrivilegesConfirmOptions,
};

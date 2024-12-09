import serverSettings from "common/settings/serverSettings";
import { TextColor } from "components/models/common";
import {
    CertificatesClearance,
    CertificatesState,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";

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

function getState(notAfter: Date): CertificatesState {
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

export const certificateUtils = {
    getClearance,
    getState,
    getStateColor,
    getStateDateColor,
};

import { createSelector } from "@reduxjs/toolkit";
import { orderBy } from "common/typeUtils";
import { InputItem } from "components/models/common";
import {
    CertificatesClearance,
    CertificatesManagementType,
    CertificatesState,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { RootState } from "components/store";

const selectClearanceFilterOptions = createSelector(
    (state: RootState) => state.certificates.certificates,
    (certificates): InputItem<CertificatesClearance>[] => {
        let userCount = 0,
            operatorCount = 0,
            adminCount = 0;

        certificates.forEach(({ SecurityClearance }) => {
            if (SecurityClearance === "ValidUser") {
                userCount++;
            } else if (SecurityClearance === "Operator") {
                operatorCount++;
            } else if (SecurityClearance === "ClusterNode" || SecurityClearance === "ClusterAdmin") {
                adminCount++;
            }
        });

        return [
            { value: "Admin", label: "Admin", count: adminCount },
            { value: "Operator", label: "Operator", count: operatorCount },
            { value: "User", label: "User", count: userCount },
        ];
    }
);

const selectStateFilterOptions = createSelector(
    (state: RootState) => state.certificates.certificates,
    (certificates): InputItem<CertificatesState>[] => {
        let validCount = 0,
            aboutToExpireCount = 0,
            expiredCount = 0,
            disabledCount = 0;

        certificates.forEach((cert) => {
            const state = certificatesUtils.getState(cert);

            if (state === "Valid") {
                validCount++;
            }
            if (state === "About to expire") {
                aboutToExpireCount++;
                // About to expire certificates are still valid
                validCount++;
            }
            if (state === "Expired") {
                expiredCount++;
            }
            if (state === "Disabled") {
                disabledCount++;
            }
        });

        return [
            { value: "Valid", label: "Valid", count: validCount },
            { value: "About to expire", label: "About to expire", count: aboutToExpireCount },
            { value: "Expired", label: "Expired", count: expiredCount },
            { value: "Disabled", label: "Disabled", count: disabledCount },
        ];
    }
);

const selectManagementTypeFilterOptions = createSelector(
    (state: RootState) => state.certificates.certificates,
    (state: RootState) => state.certificates.serverCertificateThumbprint,
    (state: RootState) => state.certificates.serverCertificateForCommunicationThumbprint,
    (certificates, serverThumbprint, serverForCommThumbprint): InputItem<CertificatesManagementType>[] => {
        let ssoCount = 0,
            clientCount = 0,
            serverCount = 0;

        certificates.forEach((cert) => {
            if (cert.Usage === "SsoServer" || cert.Usage === "SsoClient") {
                ssoCount++;
            } else if (
                cert.Thumbprints.includes(serverThumbprint) ||
                cert.Thumbprints.includes(serverForCommThumbprint)
            ) {
                serverCount++;
            } else {
                clientCount++;
            }
        });

        return [
            { value: "SSO", label: "SSO", count: ssoCount },
            { value: "Client", label: "Client", count: clientCount },
            { value: "Server", label: "Server", count: serverCount },
        ];
    }
);

const selectFilteredCertificates = createSelector(
    (state: RootState) => state.certificates.certificates,
    (state: RootState) => state.certificates.nameOrThumbprintFilter,
    (state: RootState) => state.certificates.databaseFilter,
    (state: RootState) => state.certificates.clearanceFilter,
    (state: RootState) => state.certificates.stateFilter,
    (state: RootState) => state.certificates.managementTypeFilter,
    (state: RootState) => state.certificates.sortMode,
    (state: RootState) => state.certificates.serverCertificateThumbprint,
    (state: RootState) => state.certificates.serverCertificateForCommunicationThumbprint,
    (
        certificates,
        nameOrThumbprintFilter,
        databaseFilter,
        clearanceFilter,
        stateFilter,
        managementTypeFilter,
        sortMode,
        serverThumbprint,
        serverForCommThumbprint
    ) => {
        const filteredCertificates = certificates.filter((cert) => {
            if (
                nameOrThumbprintFilter &&
                !cert.Name.toLowerCase().includes(nameOrThumbprintFilter.toLowerCase()) &&
                !cert.Thumbprints.some((x) => x.toLowerCase().includes(nameOrThumbprintFilter.toLowerCase()))
            ) {
                return false;
            }

            const permissionKeys = Object.keys(cert.Permissions);
            if (databaseFilter && permissionKeys.length > 0 && !permissionKeys.includes(databaseFilter)) {
                return false;
            }

            if (
                clearanceFilter.length > 0 &&
                !clearanceFilter.includes(certificatesUtils.getClearance(cert.SecurityClearance))
            ) {
                return false;
            }

            const state = certificatesUtils.getState(cert);
            if (stateFilter.includes("Valid") && (state === "Valid" || state === "About to expire")) {
                return true;
            }

            if (stateFilter.length > 0 && !stateFilter.includes(state)) {
                return false;
            }

            if (managementTypeFilter.length > 0) {
                const isServer =
                    cert.Thumbprints.includes(serverThumbprint) ||
                    cert.Thumbprints.includes(serverForCommThumbprint);
                const isSso = cert.Usage === "SsoServer" || cert.Usage === "SsoClient";
                const isClient = !isServer && !isSso;

                const matchesFilter =
                    (managementTypeFilter.includes("SSO") && isSso) ||
                    (managementTypeFilter.includes("Client") && isClient) ||
                    (managementTypeFilter.includes("Server") && isServer);

                if (!matchesFilter) {
                    return false;
                }
            }

            return true;
        });

        switch (sortMode) {
            case "By Name - Asc":
                return orderBy(filteredCertificates, (cert) => cert.Name, ["asc"]);
            case "By Name - Desc":
                return orderBy(filteredCertificates, (cert) => cert.Name, ["desc"]);
            case "By Expiration Date - Asc":
                return orderBy(filteredCertificates, (cert) => cert.NotAfter, ["asc"]);
            case "By Expiration Date - Desc":
                return orderBy(filteredCertificates, (cert) => cert.NotAfter, ["desc"]);
            case "By Valid-From Date - Asc":
                return orderBy(filteredCertificates, (cert) => cert.NotBefore, ["asc"]);
            case "By Valid-From Date - Desc":
                return orderBy(filteredCertificates, (cert) => cert.NotBefore, ["desc"]);
            case "By Last Used Date - Asc":
                return orderBy(filteredCertificates, (cert) => cert.LastUsedDate, ["asc"]);
            case "By Last Used Date - Desc":
                return orderBy(filteredCertificates, (cert) => cert.LastUsedDate, ["desc"]);
        }
    }
);

const selectHasClusterNodeCertificate = createSelector(
    (state: RootState) => state.certificates.certificates,
    (certificates): boolean => {
        return certificates.some((x) => x.SecurityClearance === "ClusterNode");
    }
);

const selectSsoServerCertificates = createSelector(
    (state: RootState) => state.certificates.certificates,
    (certificates) => certificates.filter((c) => c.Usage === "SsoServer")
);

const selectSsoUserCertificates = createSelector(
    (state: RootState) => state.certificates.certificates,
    (certificates) => certificates.filter((c) => c.Usage === "SsoClient")
);

export const certificatesSelectors = {
    certificates: (state: RootState) => state.certificates.certificates,
    isInitialLoad: (state: RootState) => state.certificates.isInitialLoad,
    loadStatus: (state: RootState) => state.certificates.loadStatus,
    filteredCertificates: selectFilteredCertificates,
    hasClusterNodeCertificate: selectHasClusterNodeCertificate,
    wellKnownAdminCerts: (state: RootState) => state.certificates.wellKnownAdminCerts,
    wellKnownIssuers: (state: RootState) => state.certificates.wellKnownIssuers,
    serverCertificateThumbprint: (state: RootState) => state.certificates.serverCertificateThumbprint,
    serverCertificateForCommunicationThumbprint: (state: RootState) =>
        state.certificates.serverCertificateForCommunicationThumbprint,
    serverCertificateSetupMode: (state: RootState) => state.certificates.serverCertificateSetupMode,
    serverCertificateRenewalDate: (state: RootState) => state.certificates.serverCertificateRenewalDate,
    nameOrThumbprintFilter: (state: RootState) => state.certificates.nameOrThumbprintFilter,
    databaseFilter: (state: RootState) => state.certificates.databaseFilter,
    clearanceFilter: (state: RootState) => state.certificates.clearanceFilter,
    clearanceFilterOptions: selectClearanceFilterOptions,
    stateFilter: (state: RootState) => state.certificates.stateFilter,
    stateFilterOptions: selectStateFilterOptions,
    managementTypeFilter: (state: RootState) => state.certificates.managementTypeFilter,
    managementTypeFilterOptions: selectManagementTypeFilterOptions,
    sortMode: (state: RootState) => state.certificates.sortMode,
    isGenerateModalOpen: (state: RootState) => state.certificates.isGenerateModalOpen,
    isUploadModalOpen: (state: RootState) => state.certificates.isUploadModalOpen,
    certificateToEdit: (state: RootState) => state.certificates.certificateToEdit,
    certificateToClone: (state: RootState) => state.certificates.certificateToClone,
    isReplaceServerModalOpen: (state: RootState) => state.certificates.isReplaceServerModalOpen,
    isRegisterSsoServerModalOpen: (state: RootState) => state.certificates.isRegisterSsoServerModalOpen,
    isRegisterSsoUserModalOpen: (state: RootState) => state.certificates.isRegisterSsoUserModalOpen,
    ssoUserToEdit: (state: RootState) => state.certificates.ssoUserToEdit,
    ssoServerCertificates: selectSsoServerCertificates,
    ssoUserCertificates: selectSsoUserCertificates,
};

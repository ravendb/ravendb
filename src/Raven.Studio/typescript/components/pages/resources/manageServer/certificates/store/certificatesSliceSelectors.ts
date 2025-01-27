import { createSelector } from "@reduxjs/toolkit";
import { orderBy } from "common/typeUtils";
import { InputItem } from "components/models/common";
import {
    CertificatesClearance,
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
            { value: "User", label: "User", count: userCount },
            { value: "Operator", label: "Operator", count: operatorCount },
            { value: "Admin", label: "Admin", count: adminCount },
        ];
    }
);

const selectStateFilterOptions = createSelector(
    (state: RootState) => state.certificates.certificates,
    (certificates): InputItem<CertificatesState>[] => {
        let validCount = 0,
            aboutToExpireCount = 0,
            expiredCount = 0;

        certificates.forEach(({ NotAfter }) => {
            const state = certificatesUtils.getState(NotAfter);

            if (state === "Valid") {
                validCount++;
            }
            if (state === "About to expire") {
                aboutToExpireCount++;
            }
            if (state === "Expired") {
                expiredCount++;
            }
        });

        return [
            { value: "Valid", label: "Valid", count: validCount },
            { value: "About to expire", label: "About to expire", count: aboutToExpireCount },
            { value: "Expired", label: "Expired", count: expiredCount },
        ];
    }
);

const selectFilteredCertificates = createSelector(
    (state: RootState) => state.certificates.certificates,
    (state: RootState) => state.certificates.nameOrThumbprintFilter,
    (state: RootState) => state.certificates.databaseFilter,
    (state: RootState) => state.certificates.clearanceFilter,
    (state: RootState) => state.certificates.stateFilter,
    (state: RootState) => state.certificates.sortMode,
    (certificates, nameOrThumbprintFilter, databaseFilter, clearanceFilter, stateFilter, sortMode) => {
        const filteredCertificates = certificates.filter((cert) => {
            if (
                nameOrThumbprintFilter &&
                !cert.Name.toLowerCase().includes(nameOrThumbprintFilter.toLowerCase()) &&
                !cert.Thumbprint.toLowerCase().includes(nameOrThumbprintFilter.toLowerCase())
            ) {
                return false;
            }

            if (databaseFilter && !Object.keys(cert.Permissions).includes(databaseFilter)) {
                return false;
            }

            if (
                clearanceFilter.length > 0 &&
                !clearanceFilter.includes(certificatesUtils.getClearance(cert.SecurityClearance))
            ) {
                return false;
            }

            if (stateFilter.length > 0 && !stateFilter.includes(certificatesUtils.getState(cert.NotAfter))) {
                return false;
            }

            return true;
        });

        switch (sortMode) {
            case "Default":
                return filteredCertificates;
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

export const certificatesSelectors = {
    certificates: (state: RootState) => state.certificates.certificates,
    filteredCertificates: selectFilteredCertificates,
    hasClusterNodeCertificate: selectHasClusterNodeCertificate,
    serverCertificateThumbprint: (state: RootState) => state.certificates.serverCertificateThumbprint,
    serverCertificateSetupMode: (state: RootState) => state.certificates.serverCertificateSetupMode,
    serverCertificateRenewalDate: (state: RootState) => state.certificates.serverCertificateRenewalDate,
    nameOrThumbprintFilter: (state: RootState) => state.certificates.nameOrThumbprintFilter,
    databaseFilter: (state: RootState) => state.certificates.databaseFilter,
    clearanceFilter: (state: RootState) => state.certificates.clearanceFilter,
    clearanceFilterOptions: selectClearanceFilterOptions,
    stateFilter: (state: RootState) => state.certificates.stateFilter,
    stateFilterOptions: selectStateFilterOptions,
    sortMode: (state: RootState) => state.certificates.sortMode,
    isGenerateModalOpen: (state: RootState) => state.certificates.isGenerateModalOpen,
    isUploadModalOpen: (state: RootState) => state.certificates.isUploadModalOpen,
    certificateToRegenerate: (state: RootState) => state.certificates.certificateToRegenerate,
    certificateToEdit: (state: RootState) => state.certificates.certificateToEdit,
    isReplaceServerModalOpen: (state: RootState) => state.certificates.isReplaceServerModalOpen,
};

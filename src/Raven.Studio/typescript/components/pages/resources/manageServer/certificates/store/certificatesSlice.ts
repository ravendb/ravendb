import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import {
    CertificateItem,
    CertificatesClearance,
    CertificatesManagementType,
    CertificatesSortMode,
    CertificatesState,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { RootState } from "components/store";

interface InitialState {
    certificates: CertificateItem[];
    isInitialLoad: boolean;
    loadStatus: loadStatus;
    serverCertificateThumbprint: string;
    serverCertificateForCommunicationThumbprint: string;
    serverCertificateRenewalDate: string;
    serverCertificateSetupMode: Raven.Server.Commercial.SetupMode;
    wellKnownAdminCerts: string[];
    wellKnownIssuers: string[];
    nameOrThumbprintFilter: string;
    databaseFilter: string;
    clearanceFilter: CertificatesClearance[];
    stateFilter: CertificatesState[];
    managementTypeFilter: CertificatesManagementType[];
    sortMode: CertificatesSortMode;
    isGenerateModalOpen: boolean;
    isUploadModalOpen: boolean;
    certificateToEdit: CertificateItem;
    certificateToClone: CertificateItem;
    isReplaceServerModalOpen: boolean;
    isRegisterSsoServerModalOpen: boolean;
    isRegisterSsoUserModalOpen: boolean;
    ssoUserToEdit: CertificateItem;
    ssoUserToClone: CertificateItem;
}

const initialState: InitialState = {
    certificates: [],
    loadStatus: "idle",
    isInitialLoad: true,
    serverCertificateThumbprint: null,
    serverCertificateForCommunicationThumbprint: null,
    serverCertificateRenewalDate: null,
    serverCertificateSetupMode: null,
    wellKnownAdminCerts: [],
    wellKnownIssuers: [],
    nameOrThumbprintFilter: "",
    databaseFilter: "",
    clearanceFilter: [],
    stateFilter: [],
    managementTypeFilter: [],
    sortMode: "By Name - Asc",
    isGenerateModalOpen: false,
    isUploadModalOpen: false,
    certificateToEdit: null,
    certificateToClone: null,
    isReplaceServerModalOpen: false,
    isRegisterSsoServerModalOpen: false,
    isRegisterSsoUserModalOpen: false,
    ssoUserToEdit: null,
    ssoUserToClone: null,
};

export const certificatesSlice = createSlice({
    name: "certificates",
    initialState,
    reducers: {
        nameOrThumbprintFilterSet: (state, action: PayloadAction<string>) => {
            state.nameOrThumbprintFilter = action.payload;
        },
        databaseFilterSet: (state, action: PayloadAction<string>) => {
            state.databaseFilter = action.payload;
        },
        clearanceFilterSet: (state, action: PayloadAction<CertificatesClearance[]>) => {
            state.clearanceFilter = action.payload;
        },
        stateFilterSet: (state, action: PayloadAction<CertificatesState[]>) => {
            state.stateFilter = action.payload;
        },
        managementTypeFilterSet: (state, action: PayloadAction<CertificatesManagementType[]>) => {
            state.managementTypeFilter = action.payload;
        },
        sortModeSet: (state, action: PayloadAction<CertificatesSortMode>) => {
            state.sortMode = action.payload;
        },
        isGenerateModalOpenToggled: (state) => {
            state.isGenerateModalOpen = !state.isGenerateModalOpen;
        },
        isUploadModalOpenToggled: (state) => {
            state.isUploadModalOpen = !state.isUploadModalOpen;
        },
        editModalOpen: (state, action: PayloadAction<CertificateItem>) => {
            state.certificateToEdit = action.payload;
        },
        editModalClosed: (state) => {
            state.certificateToEdit = null;
        },
        cloneModalOpen: (state, action: PayloadAction<CertificateItem>) => {
            state.certificateToClone = action.payload;
        },
        cloneModalClosed: (state) => {
            state.certificateToClone = null;
        },
        isReplaceServerModalOpenToggled: (state) => {
            state.isReplaceServerModalOpen = !state.isReplaceServerModalOpen;
        },
        isRegisterSsoServerModalOpenToggled: (state) => {
            state.isRegisterSsoServerModalOpen = !state.isRegisterSsoServerModalOpen;
        },
        isRegisterSsoUserModalOpenToggled: (state) => {
            state.isRegisterSsoUserModalOpen = !state.isRegisterSsoUserModalOpen;
            state.ssoUserToEdit = null;
            state.ssoUserToClone = null;
        },
        ssoUserEditModalOpen: (state, action: PayloadAction<CertificateItem>) => {
            state.ssoUserToEdit = action.payload;
            state.ssoUserToClone = null;
            state.isRegisterSsoUserModalOpen = true;
        },
        ssoUserCloneModalOpen: (state, action: PayloadAction<CertificateItem>) => {
            state.ssoUserToClone = action.payload;
            state.ssoUserToEdit = null;
            state.isRegisterSsoUserModalOpen = true;
        },
    },
    extraReducers: (builder) => {
        builder.addCase(fetchData.fulfilled, (state, action: PayloadAction<FetchDataLastUsedResult>) => {
            const { lastUsed, certificatesDto, serverCertificateSetupMode, serverCertificateRenewalDate } =
                action.payload;

            state.certificates = certificatesDto.Certificates.filter((x) => !x.CollectionPrimaryKey).map((cert) => ({
                ...cert,
                Thumbprints: [cert.Thumbprint],
                LastUsedDate: lastUsed[cert.Thumbprint] ?? null,
            }));

            // secondary certs
            certificatesDto.Certificates.filter((x) => x.CollectionPrimaryKey).forEach((cert) => {
                const thumbprint = cert.CollectionPrimaryKey;
                const primaryCert = state.certificates.find((x) => x.Thumbprint === thumbprint);

                if (primaryCert) {
                    primaryCert.Thumbprints.push(cert.Thumbprint);
                }
            });

            state.serverCertificateRenewalDate = serverCertificateRenewalDate;
            state.serverCertificateSetupMode = serverCertificateSetupMode;
            state.serverCertificateThumbprint = certificatesDto.LoadedServerCert;
            state.serverCertificateForCommunicationThumbprint = certificatesDto.LoadedServerCertForCommunication;
            state.wellKnownAdminCerts = certificatesDto.WellKnownAdminCerts ?? [];
            state.wellKnownIssuers = certificatesDto.WellKnownIssuers ?? [];

            state.loadStatus = "success";
            state.isInitialLoad = false;
        });
        builder.addCase(fetchData.rejected, (state) => {
            state.loadStatus = "failure";
        });
        builder.addCase(fetchData.pending, (state) => {
            state.loadStatus = "loading";
        });
    },
});

interface FetchDataLastUsedResult {
    certificatesDto: CertificatesResponseDto;
    lastUsed: Record<string, string>;
    serverCertificateSetupMode: Raven.Server.Commercial.SetupMode;
    serverCertificateRenewalDate: string;
}

const fetchData = createAsyncThunk<
    FetchDataLastUsedResult,
    undefined,
    {
        state: RootState;
    }
>(certificatesSlice.name + "/fetchData", async (_, { getState }) => {
    const nodeTags = getState().cluster.nodes.ids;
    const securityClearance = getState().accessManager.securityClearance;
    const isClusterAdminOrClusterNode = securityClearance === "ClusterAdmin" || securityClearance === "ClusterNode";

    const certificatesDto = await services.manageServerService.getCertificates(true);

    const serverCertificateSetupMode = isClusterAdminOrClusterNode
        ? await services.manageServerService.getServerCertificateSetupMode()
        : null;

    const serverCertificateRenewalDate =
        serverCertificateSetupMode === "LetsEncrypt"
            ? await services.manageServerService.getServerCertificateRenewalDate()
            : null;

    const statsTasks = nodeTags.map(async (tag) => {
        try {
            const stats = await services.manageServerService.getAdminStats(tag);
            return stats.LastRequestTimePerCertificate;
        } catch (e) {
            // we ignore errors here
            return {};
        }
    });

    const allStats = await Promise.all(statsTasks);

    const lastUsedResult: Record<string, string> = {};
    allStats.forEach((nodeStats) => {
        Object.keys(nodeStats).forEach((thumbprint) => {
            const lastUsed = nodeStats[thumbprint];

            if (!lastUsedResult[thumbprint] || lastUsedResult[thumbprint].localeCompare(lastUsed) > 0) {
                lastUsedResult[thumbprint] = lastUsed;
            }
        });
    });

    return {
        certificatesDto,
        lastUsed: lastUsedResult,
        serverCertificateRenewalDate,
        serverCertificateSetupMode,
    };
});

export const certificatesActions = {
    ...certificatesSlice.actions,
    fetchData,
};

import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import {
    CertificateItem,
    CertificatesSortMode,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { RootState } from "components/store";

interface InitialState {
    certificates: CertificateItem[];
    certificatesLoadStatus: loadStatus;
    loadedServerCert: string;
    wellKnownAdminCerts: string[];
    wellKnownIssuers: string[];
    nameOrThumbprintFilter: string;
    databaseFilter: string;
    clearanceFilter: string;
    stateFilter: string;
    sortMode: CertificatesSortMode;
}

const initialState: InitialState = {
    certificates: [],
    certificatesLoadStatus: "idle",
    loadedServerCert: null,
    wellKnownAdminCerts: [],
    wellKnownIssuers: [],
    nameOrThumbprintFilter: "",
    databaseFilter: "",
    clearanceFilter: "",
    stateFilter: "",
    sortMode: "Default",
};

const certificatesSlice = createSlice({
    name: "certificates",
    initialState,
    reducers: {
        nameOrThumbprintFilterSet: (state, action: PayloadAction<string>) => {
            state.nameOrThumbprintFilter = action.payload;
        },
        databaseFilterSet: (state, action: PayloadAction<string>) => {
            state.databaseFilter = action.payload;
        },
        clearanceFilterSet: (state, action: PayloadAction<string>) => {
            state.clearanceFilter = action.payload;
        },
        stateFilterSet: (state, action: PayloadAction<string>) => {
            state.stateFilter = action.payload;
        },
        sortModeSet: (state, action: PayloadAction<CertificatesSortMode>) => {
            state.sortMode = action.payload;
        },
    },
    extraReducers: (builder) => {
        builder.addCase(
            fetchData.fulfilled,
            (state, { payload: { lastUsed, certificatesDto } }: PayloadAction<FetchDataLastUsedResult>) => {
                // state.certificates = action.payload.certificatesDto.certificates;

                // Fill last used date
                Object.keys(lastUsed).map((thumbprint) => {
                    const lastUsedDate = lastUsed[thumbprint];

                    const certificateToApply = state.certificates.find((x) => x.Thumbprint === thumbprint);
                    if (certificateToApply) {
                        certificateToApply.LastUsedDate = lastUsedDate;
                    }
                });

                state.certificatesLoadStatus = "success";
            }
        );
        builder.addCase(fetchData.rejected, (state) => {
            state.certificatesLoadStatus = "failure";
        });
        builder.addCase(fetchData.pending, (state) => {
            state.certificatesLoadStatus = "loading";
        });
    },
});

interface FetchDataLastUsedResult {
    certificatesDto: CertificatesResponseDto;
    lastUsed: Record<string, string>;
}

const fetchData = createAsyncThunk<
    FetchDataLastUsedResult,
    unknown,
    {
        state: RootState;
    }
>(certificatesSlice.name + "/fetchData", async (_, { getState }) => {
    const nodeTags = getState().cluster.nodes.ids;

    const certificatesDto = await services.manageServerService.getCertificates(true);

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
    };
});

export const certificatesActions = {
    ...certificatesSlice.actions,
};

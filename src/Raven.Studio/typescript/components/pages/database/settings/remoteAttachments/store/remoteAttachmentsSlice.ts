import { createAsyncThunk, createEntityAdapter, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { services } from "hooks/useServices";
import { loadStatus } from "components/models/common";
import { RemoteAttachmentsDestinationFormData, RemoteAttachmentsFormData } from "../remoteAttachmentsValidation";
import { remoteAttachmentsUtils } from "../remoteAttachmentsUtils";
import RemoteAttachmentsConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration;

const destinationsAdapter = createEntityAdapter<RemoteAttachmentsDestinationFormData, string>({
    selectId: (destination) => destination.identifier,
    sortComparer: (a, b) => a.identifier.localeCompare(b.identifier),
});

export interface RemoteAttachmentsState {
    loadStatus: loadStatus;
    initialDestinations: ReturnType<typeof destinationsAdapter.getInitialState>;
    destinations: ReturnType<typeof destinationsAdapter.getInitialState>;
}

const initialState: RemoteAttachmentsState = {
    loadStatus: "idle",
    initialDestinations: destinationsAdapter.getInitialState(),
    destinations: destinationsAdapter.getInitialState(),
};

export const fetchRemoteAttachments = createAsyncThunk("remoteAttachments/fetch", async (databaseName: string) => {
    const dto = await services.databasesService.getRemoteAttachmentsConfiguration(databaseName);
    return dto as RemoteAttachmentsConfiguration;
});

type RemoteAttachmentsPayload = RemoteAttachmentsFormData & { destinations: RemoteAttachmentsDestinationFormData[] };

export const saveRemoteAttachments = createAsyncThunk(
    "remoteAttachments/save",
    async (args: { databaseName: string; data: RemoteAttachmentsPayload }, { rejectWithValue }) => {
        try {
            const dto = remoteAttachmentsUtils.mapToDto(args.data);

            await services.databasesService.saveRemoteAttachmentsConfiguration(args.databaseName, dto);
            return args.data;
        } catch (e) {
            return rejectWithValue(e);
        }
    }
);

export const remoteAttachmentsSlice = createSlice({
    name: "remoteAttachments",
    initialState,
    reducers: {
        addDestination: (state, { payload }: PayloadAction<RemoteAttachmentsDestinationFormData>) => {
            destinationsAdapter.addOne(state.destinations, payload);
        },
        updateDestination: (
            state,
            { payload }: PayloadAction<{ prevId: string; destination: RemoteAttachmentsDestinationFormData }>
        ) => {
            const { prevId, destination } = payload;
            if (prevId !== destination.identifier) {
                destinationsAdapter.removeOne(state.destinations, prevId);
                destinationsAdapter.addOne(state.destinations, destination);
            } else {
                destinationsAdapter.upsertOne(state.destinations, destination);
            }
        },
        removeDestination: (state, { payload }: PayloadAction<string>) => {
            destinationsAdapter.removeOne(state.destinations, payload);
        },
        toggleDestinationDisabled: (state, { payload }: PayloadAction<string>) => {
            const entity = state.destinations.entities[payload];
            if (entity) {
                destinationsAdapter.updateOne(state.destinations, {
                    id: payload,
                    changes: { disabled: !entity.disabled },
                });
            }
        },
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchRemoteAttachments.pending, (state) => {
                state.loadStatus = "loading";
            })
            .addCase(fetchRemoteAttachments.fulfilled, (state, { payload }) => {
                const form = remoteAttachmentsUtils.mapFromDto(payload);

                state.loadStatus = "success";
                destinationsAdapter.setAll(state.destinations, form.destinations);
                destinationsAdapter.setAll(state.initialDestinations, form.destinations);
            })
            .addCase(fetchRemoteAttachments.rejected, (state) => {
                state.loadStatus = "failure";
            })
            .addCase(saveRemoteAttachments.fulfilled, (state, { payload }) => {
                destinationsAdapter.setAll(state.destinations, payload.destinations);
                destinationsAdapter.setAll(state.initialDestinations, payload.destinations);
            });
    },
});

export const destinationsSelectors = destinationsAdapter.getSelectors(
    (state: RemoteAttachmentsState) => state.destinations
);

export const initialDestinationsSelectors = destinationsAdapter.getSelectors(
    (state: RemoteAttachmentsState) => state.initialDestinations
);

export const remoteAttachmentsActions = {
    ...remoteAttachmentsSlice.actions,
    fetchRemoteAttachments,
    saveRemoteAttachments,
};

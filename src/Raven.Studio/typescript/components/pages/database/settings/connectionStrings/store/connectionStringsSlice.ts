import { PayloadAction, createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import { Connection, StudioConnectionType } from "../connectionStringsTypes";
import { RootState } from "components/store";
import { ConnectionStringsUrlParameters } from "../ConnectionStrings";
import {
    mapAllConnectionsFromDto,
    mapServerWideConnectionsFromDto,
    ServerWideConnectionStringDto,
} from "./connectionStringsMapsFromDto";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export type ConnectionStringsViewContext =
    | "connectionStrings"
    | "aiConnectionStrings"
    | "aiTask"
    | "serverWideConnectionStrings";

interface ConnectionStringsState {
    loadStatus: loadStatus;
    connections: { [key in StudioConnectionType]: Connection[] };
    urlParameters: ConnectionStringsUrlParameters;
    initialEditConnection: Connection;
    viewContext: ConnectionStringsViewContext;
}

const initialState: ConnectionStringsState = {
    loadStatus: "idle",
    connections: {
        Raven: [],
        Sql: [],
        Snowflake: [],
        Olap: [],
        ElasticSearch: [],
        Kafka: [],
        RabbitMQ: [],
        AzureQueueStorage: [],
        AmazonSqs: [],
        AzureServiceBus: [],
        Ai: [],
    },
    urlParameters: {
        name: null,
        type: null,
    },
    initialEditConnection: null,
    viewContext: "connectionStrings",
};

export const connectionStringsSlice = createSlice({
    name: "connectionStrings",
    initialState,
    reducers: {
        urlParametersLoaded: (state, { payload: urlParameters }: PayloadAction<ConnectionStringsUrlParameters>) => {
            state.urlParameters = urlParameters;
        },
        newConnectionModalOpened: (state) => {
            state.initialEditConnection = { type: null };
        },
        newConnectionOfTypeModalOpened: (state, { payload: type }: PayloadAction<StudioConnectionType>) => {
            state.initialEditConnection = { type };
        },
        editConnectionModalOpened: (state, { payload: connection }: PayloadAction<Connection>) => {
            state.initialEditConnection = connection;
        },
        serverWideEditConnectionOpened: (state, { payload: connection }: PayloadAction<Connection>) => {
            state.initialEditConnection = connection;
        },
        editConnectionModalClosed: (state) => {
            state.initialEditConnection = null;
        },
        connectionAdded: (state, { payload: connection }: PayloadAction<Connection>) => {
            const newConnection: Connection = {
                ...connection,
                usedByTasks: connection.usedByTasks ?? [],
            };

            state.connections[connection.type].push(newConnection);
        },
        connectionEdited: (state, { payload }: PayloadAction<{ oldName: string; newConnection: Connection }>) => {
            const type = payload.newConnection.type;

            state.connections[type] = state.connections[type].map((x) =>
                x.name === payload.oldName ? payload.newConnection : x
            );
        },
        connectionDeleted: (state, { payload }: PayloadAction<Connection>) => {
            state.connections[payload.type] = state.connections[payload.type].filter((x) => x.name !== payload.name);
        },
        viewContextSet: (state, { payload: viewContext }: PayloadAction<ConnectionStringsViewContext>) => {
            state.viewContext = viewContext;
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchData.fulfilled, (state, { payload }) => {
                const { connectionStringsDto } = payload;
                const { urlParameters } = state;

                state.connections = mapAllConnectionsFromDto(connectionStringsDto);
                state.loadStatus = "success";

                if (payload.hasDatabaseAdminAccess && urlParameters.name && urlParameters.type) {
                    const foundConnection = state.connections?.[urlParameters.type]?.find(
                        (x) => x?.name === urlParameters.name
                    );

                    state.initialEditConnection = foundConnection ?? null;
                }
            })
            .addCase(fetchData.pending, (state) => {
                state.loadStatus = "loading";
            })
            .addCase(fetchData.rejected, (state) => {
                state.loadStatus = "failure";
            })
            .addCase(fetchServerWideData.fulfilled, (state, { payload }) => {
                state.connections = mapServerWideConnectionsFromDto(payload.serverWideDto);
                state.loadStatus = "success";
            })
            .addCase(fetchServerWideData.pending, (state) => {
                state.loadStatus = "loading";
            })
            .addCase(fetchServerWideData.rejected, (state) => {
                state.loadStatus = "failure";
            });
    },
});

interface FetchDataResult {
    connectionStringsDto: GetConnectionStringsResult;
    hasDatabaseAdminAccess: boolean;
}

interface FetchServerWideDataResult {
    serverWideDto: ServerWideConnectionStringDto[];
}

const fetchData = createAsyncThunk<
    FetchDataResult,
    string,
    {
        state: RootState;
    }
>(connectionStringsSlice.name + "/fetchConnectionStrings", async (databaseName, { getState }) => {
    const state = getState();

    const db = databaseSelectors.databaseByName(databaseName)(state);

    const connectionStringsDto = await services.tasksService.getConnectionStrings(db.name);

    const hasDatabaseAdminAccess = accessManagerSelectors.getHasDatabaseAdminAccess(state)(db.name);

    return {
        connectionStringsDto,
        hasDatabaseAdminAccess,
    };
});

const fetchServerWideData = createAsyncThunk<FetchServerWideDataResult, void>(
    connectionStringsSlice.name + "/fetchServerWideConnectionStrings",
    async () => {
        const { Results } = await services.tasksService.getServerWideConnectionStrings();
        return { serverWideDto: Results };
    }
);

export const connectionStringsActions = {
    ...connectionStringsSlice.actions,
    fetchData,
    fetchServerWideData,
};

export const connectionStringSelectors = {
    loadStatus: (store: RootState) => store.connectionStrings.loadStatus,
    connections: (store: RootState) => store.connectionStrings.connections,
    initialEditConnection: (store: RootState) => store.connectionStrings.initialEditConnection,
    isEmpty: (store: RootState) => _.isEqual(store.connectionStrings.connections, initialState.connections),
    viewContext: (store: RootState) => store.connectionStrings.viewContext,
    isServerWide: (store: RootState) => store.connectionStrings.viewContext === "serverWideConnectionStrings",
};

import { PayloadAction, createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import { Connection } from "../connectionStringsTypes";
import { RootState } from "components/store";
import { ConnectionStringsUrlParameters } from "../ConnectionStrings";
import {
    mapElasticSearchConnectionsFromDto,
    mapKafkaConnectionsFromDto,
    mapOlapConnectionsFromDto,
    mapRabbitMqConnectionsFromDto,
    mapRavenConnectionsFromDto,
    mapSqlConnectionsFromDto,
} from "./connectionStringsMapsFromDto";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

interface ConnectionStringsState {
    loadStatus: loadStatus;
    connections: { [key in StudioEtlType]: Connection[] };
    urlParameters: ConnectionStringsUrlParameters;
    initialEditConnection: Connection;
}

const initialState: ConnectionStringsState = {
    loadStatus: "idle",
    connections: {
        Raven: [],
        Sql: [],
        Olap: [],
        ElasticSearch: [],
        Kafka: [],
        RabbitMQ: [],
    },
    urlParameters: {
        name: null,
        type: null,
    },
    initialEditConnection: null,
};

type StudioEtlType = "Raven" | "Sql" | "Olap" | "ElasticSearch" | "Kafka" | "RabbitMQ";

export const connectionStringsSlice = createSlice({
    name: "connectionStrings",
    initialState,
    reducers: {
        urlParametersLoaded: (state, { payload: urlParameters }: PayloadAction<ConnectionStringsUrlParameters>) => {
            state.urlParameters = urlParameters;
        },
        openAddNewConnectionModal: (state) => {
            state.initialEditConnection = { type: null };
        },
        openAddNewConnectionOfTypeModal: (state, { payload: type }: PayloadAction<StudioEtlType>) => {
            state.initialEditConnection = { type };
        },
        openEditConnectionModal: (state, { payload: connection }: PayloadAction<Connection>) => {
            state.initialEditConnection = connection;
        },
        closeEditConnectionModal: (state) => {
            state.initialEditConnection = null;
        },
        addConnection: (state, { payload: connection }: PayloadAction<Connection>) => {
            const newConnection: Connection = {
                ...connection,
                usedByTasks: connection.usedByTasks ?? [],
            };

            state.connections[connection.type].push(newConnection);
        },
        editConnection: (state, { payload }: PayloadAction<{ oldName: string; newConnection: Connection }>) => {
            const type = payload.newConnection.type;

            state.connections[type] = state.connections[type].map((x) =>
                x.name === payload.oldName ? payload.newConnection : x
            );
        },
        deleteConnection: (state, { payload }: PayloadAction<Connection>) => {
            state.connections[payload.type] = state.connections[payload.type].filter((x) => x.name !== payload.name);
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchData.fulfilled, (state, { payload }) => {
                const { connectionStringsDto, ongoingTasksDto } = payload;
                const ongoingTasks = ongoingTasksDto.OngoingTasks;

                const { connections, urlParameters } = state;

                connections.Sql = mapSqlConnectionsFromDto(connectionStringsDto.SqlConnectionStrings, ongoingTasks);
                connections.Olap = mapOlapConnectionsFromDto(connectionStringsDto.OlapConnectionStrings, ongoingTasks);

                connections.Raven = mapRavenConnectionsFromDto(
                    connectionStringsDto.RavenConnectionStrings,
                    ongoingTasks
                );
                connections.ElasticSearch = mapElasticSearchConnectionsFromDto(
                    connectionStringsDto.ElasticSearchConnectionStrings,
                    ongoingTasks
                );
                connections.Kafka = mapKafkaConnectionsFromDto(
                    connectionStringsDto.QueueConnectionStrings,
                    ongoingTasks
                );
                connections.RabbitMQ = mapRabbitMqConnectionsFromDto(
                    connectionStringsDto.QueueConnectionStrings,
                    ongoingTasks
                );

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
            });
    },
});

interface FetchDataResult {
    ongoingTasksDto: Raven.Server.Web.System.OngoingTasksResult;
    connectionStringsDto: GetConnectionStringsResult;
    hasDatabaseAdminAccess: boolean;
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

    const ongoingTasksDto = await services.tasksService.getOngoingTasks(
        databaseName,
        DatabaseUtils.getFirstLocation(db, state.cluster.localNodeTag)
    );
    const connectionStringsDto = await services.tasksService.getConnectionStrings(db.name);

    const hasDatabaseAdminAccess = accessManagerSelectors.getHasDatabaseAdminAccess(state)(db.name);

    return {
        ongoingTasksDto,
        connectionStringsDto,
        hasDatabaseAdminAccess,
    };
});

export const connectionStringsActions = {
    ...connectionStringsSlice.actions,
    fetchData,
};

export const connectionStringSelectors = {
    loadStatus: (store: RootState) => store.connectionStrings.loadStatus,
    connections: (store: RootState) => store.connectionStrings.connections,
    initialEditConnection: (store: RootState) => store.connectionStrings.initialEditConnection,
    isEmpty: (store: RootState) => _.isEqual(store.connectionStrings.connections, initialState.connections),
};

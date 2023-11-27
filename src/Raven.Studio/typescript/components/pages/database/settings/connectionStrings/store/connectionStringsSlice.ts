import { PayloadAction, createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import database from "models/resources/database";
import {
    Connection,
    ConnectionStringUsedTask,
    ElasticSearchConnection,
    KafkaConnection,
    OlapConnection,
    RabbitMqConnection,
    RavenDbConnection,
    SqlConnection,
} from "../connectionStringsTypes";
import OngoingTaskRavenEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl;
import { RootState } from "components/store";
import { ConnectionStringsUrlParameters } from "../ConnectionStrings";

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

export const connectionStringsSlice = createSlice({
    name: "connectionStrings",
    initialState,
    reducers: {
        urlParametersLoaded: (state, { payload: urlParameters }: PayloadAction<ConnectionStringsUrlParameters>) => {
            state.urlParameters = urlParameters;
        },
        openAddNewConnectionModal: (state) => {
            state.initialEditConnection = { Type: null };
        },
        openAddNewConnectionOfTypeModal: (state, { payload: Type }: PayloadAction<StudioEtlType>) => {
            state.initialEditConnection = { Type };
        },
        openEditConnectionModal: (state, { payload: connection }: PayloadAction<Connection>) => {
            state.initialEditConnection = connection;
        },
        closeEditConnectionModal: (state) => {
            state.initialEditConnection = null;
        },
        addConnection: (state, { payload: connection }: PayloadAction<Connection>) => {
            state.connections[connection.Type].push(connection);
        },
        editConnection: (state, { payload }: PayloadAction<{ oldName: string; newConnection: Connection }>) => {
            const type = payload.newConnection.Type;

            state.connections[type] = state.connections[type].map((x) =>
                x.Name === payload.oldName ? payload.newConnection : x
            );
        },
        deleteConnection: (state, { payload }: PayloadAction<Connection>) => {
            state.connections[payload.Type] = state.connections[payload.Type].filter((x) => x.Name !== payload.Name);
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchData.fulfilled, (state, { payload }) => {
                const { connectionStringsDto, ongoingTasksDto } = payload;
                const ongoingTasks = ongoingTasksDto.OngoingTasks;

                const { connections, urlParameters } = state;

                connections.Raven = Object.values(connectionStringsDto.RavenConnectionStrings).map((connection) => ({
                    ...connection,
                    Type: "Raven",
                    UsedByTasks: getConnectionStringUsedTasks(ongoingTasks, "RavenEtl", connection.Name),
                })) satisfies RavenDbConnection[];

                connections.Sql = Object.values(connectionStringsDto.SqlConnectionStrings).map((connection) => ({
                    ...connection,
                    Type: "Sql",
                    UsedByTasks: getConnectionStringUsedTasks(ongoingTasks, "SqlEtl", connection.Name),
                })) satisfies SqlConnection[];

                connections.Olap = Object.values(connectionStringsDto.OlapConnectionStrings).map((connection) => ({
                    ...connection,
                    Type: "Olap",
                    UsedByTasks: getConnectionStringUsedTasks(ongoingTasks, "OlapEtl", connection.Name),
                })) satisfies OlapConnection[];

                connections.ElasticSearch = Object.values(connectionStringsDto.ElasticSearchConnectionStrings).map(
                    (connection) => ({
                        ...connection,
                        Type: "ElasticSearch",
                        UsedByTasks: getConnectionStringUsedTasks(ongoingTasks, "ElasticSearchEtl", connection.Name),
                    })
                ) satisfies ElasticSearchConnection[];

                connections.Kafka = Object.values(connectionStringsDto.QueueConnectionStrings)
                    .filter((x) => x.BrokerType === "Kafka")
                    .map((connection) => ({
                        ...connection,
                        Type: "Kafka",
                        UsedByTasks: getConnectionStringUsedTasks(ongoingTasks, "QueueEtl", connection.Name),
                    })) satisfies KafkaConnection[];

                connections.RabbitMQ = Object.values(connectionStringsDto.QueueConnectionStrings)
                    .filter((x) => x.BrokerType === "RabbitMq")
                    .map((connection) => ({
                        ...connection,
                        Type: "RabbitMQ",
                        UsedByTasks: getConnectionStringUsedTasks(ongoingTasks, "QueueEtl", connection.Name),
                    })) satisfies RabbitMqConnection[];

                state.loadStatus = "success";

                if (urlParameters.name && urlParameters.type) {
                    state.initialEditConnection =
                        state.connections[urlParameters.type]?.find((x) => x?.Name === urlParameters.name) ?? null;
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

function getConnectionStringUsedTasks(
    tasks: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask[],
    taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType,
    connectionName: string
): ConnectionStringUsedTask[] {
    return tasks
        .filter(
            (task: OngoingTaskRavenEtl) => task.TaskType === taskType && task.ConnectionStringName === connectionName
        )
        .map((x) => ({
            id: x.TaskId,
            name: x.TaskName,
        }));
}

interface FetchDataResult {
    ongoingTasksDto: Raven.Server.Web.System.OngoingTasksResult;
    connectionStringsDto: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult;
}

const fetchData = createAsyncThunk<
    FetchDataResult,
    database,
    {
        state: RootState;
    }
>(connectionStringsSlice.name + "/fetchConnectionStrings", async (db, { getState }) => {
    const state = getState();

    const ongoingTasksDto = await services.tasksService.getOngoingTasks(
        db,
        db.getFirstLocation(state.cluster.localNodeTag)
    );
    const connectionStringsDto = await services.databasesService.getConnectionStrings(db);

    return {
        ongoingTasksDto,
        connectionStringsDto,
    };
});

export const connectionStringsActions = {
    ...connectionStringsSlice.actions,
    fetchData,
};

export const connectionStringSelectors = {
    state: (store: RootState) => ({
        loadStatus: store.connectionStrings.loadStatus,
        connections: store.connectionStrings.connections,
        initialEditConnection: store.connectionStrings.initialEditConnection,
        isEmpty: _.isEqual(store.connectionStrings.connections, initialState.connections),
    }),
};

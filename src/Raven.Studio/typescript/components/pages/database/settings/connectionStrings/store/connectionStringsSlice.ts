import { PayloadAction, createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import database from "models/resources/database";
import {
    Connection,
    ConnectionStringDto,
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
    isEmpty: boolean;
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
    isEmpty: true,
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
            state.connections[payload.newConnection.Type] = state.connections[payload.newConnection.Type].map((x) =>
                x.Name === payload.oldName ? payload.newConnection : x
            );
        },
        deleteConnection: (state, { payload }: PayloadAction<Connection>) => {
            state.connections[payload.Type] = state.connections[payload.Type].filter((x) => x.Name === payload.Name);
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchData.fulfilled, (state, { payload }) => {
                const { connectionStringsDto, ongoingTasksDto } = payload;
                const ongoingTasks = ongoingTasksDto.OngoingTasks;

                const { connections, urlParameters } = state;

                connections.Raven = mapDtoToState<RavenDbConnection>(
                    connectionStringsDto.RavenConnectionStrings,
                    "Raven",
                    ongoingTasks,
                    "RavenEtl"
                );

                connections.Sql = mapDtoToState<SqlConnection>(
                    connectionStringsDto.SqlConnectionStrings,
                    "Sql",
                    ongoingTasks,
                    "SqlEtl"
                );

                connections.Olap = mapDtoToState<OlapConnection>(
                    connectionStringsDto.OlapConnectionStrings,
                    "Olap",
                    ongoingTasks,
                    "OlapEtl"
                );

                connections.ElasticSearch = mapDtoToState<ElasticSearchConnection>(
                    connectionStringsDto.ElasticSearchConnectionStrings,
                    "ElasticSearch",
                    ongoingTasks,
                    "ElasticSearchEtl"
                );

                connections.Kafka = mapDtoToState<KafkaConnection>(
                    connectionStringsDto.SqlConnectionStrings,
                    "Kafka",
                    ongoingTasks,
                    "QueueEtl"
                );

                connections.RabbitMQ = mapDtoToState<RabbitMqConnection>(
                    connectionStringsDto.SqlConnectionStrings,
                    "RabbitMQ",
                    ongoingTasks,
                    "QueueEtl"
                );

                state.isEmpty = _.isEqual(initialState.connections, state.connections);

                state.loadStatus = "success";

                if (urlParameters.name && urlParameters.type) {
                    state.initialEditConnection =
                        state.connections[urlParameters.type]?.find((x) => x.Name === urlParameters.name) ?? null;
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

function mapDtoToState<T extends Connection>(
    connectionStrings: { [key: string]: ConnectionStringDto },
    etlType: T["Type"],
    ongoingTasks: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask[],
    taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType
): T[] {
    return Object.values(connectionStrings).map((connection) => ({
        ...connection,
        Type: etlType,
        UsedByTasks: ongoingTasks
            .filter(
                (task: OngoingTaskRavenEtl) =>
                    task.TaskType === taskType && task.ConnectionStringName === connection.Name
            )
            .map((x) => ({
                id: x.TaskId,
                name: x.TaskName,
            })),
    })) as T[];
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
        isEmpty: store.connectionStrings.isEmpty,
    }),
};

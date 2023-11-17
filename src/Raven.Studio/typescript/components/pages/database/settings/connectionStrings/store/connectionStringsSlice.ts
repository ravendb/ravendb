import { PayloadAction, createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import database from "models/resources/database";
import { Connection, RavenDBConnection } from "../connectionStringsTypes";
import OngoingTaskRavenEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl;
import { RootState } from "components/store";
import { ConnectionStringsUrlParameters } from "../ConnectionStrings";

interface Connections {
    raven: RavenDBConnection[];
}

interface ConnectionStringsState {
    loadStatus: loadStatus;
    connections: Connections;
    urlParameters: ConnectionStringsUrlParameters;
    initialEditConnection: Connection;
}

const initialState: ConnectionStringsState = {
    loadStatus: "idle",
    connections: {
        raven: [],
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
        urlParametersLoaded: (state, { payload }: PayloadAction<ConnectionStringsUrlParameters>) => {
            state.urlParameters = payload;
        },
        openAddNewConnectionModal: (state) => {
            state.initialEditConnection = { Type: null };
        },
        openAddNewConnectionOfTypeModal: (state, { payload: Type }: PayloadAction<"Raven">) => {
            // TODO add types
            state.initialEditConnection = { Type };
        },
        openEditConnectionModal: (state, { payload }: PayloadAction<Connection>) => {
            state.initialEditConnection = payload;
        },
        closeEditConnectionModal: (state) => {
            state.initialEditConnection = null;
        },
        addConnection: (state, { payload }: PayloadAction<Connection>) => {
            getConnectionsOfType(state.connections, payload.Type).push(payload);
        },
        editConnection: (state, { payload }: PayloadAction<{ oldName: string; newConnection: Connection }>) => {
            const connectionsOfType = getConnectionsOfType(state.connections, payload.newConnection.Type);

            connectionsOfType.splice(
                connectionsOfType.indexOf(connectionsOfType.find((x) => x.Name === payload.oldName)),
                1,
                payload.newConnection
            );
        },
        deleteConnection: (state, { payload }: PayloadAction<{ type: "Raven"; name: string }>) => {
            const connectionsOfType = getConnectionsOfType(state.connections, payload.type);

            connectionsOfType.splice(
                connectionsOfType.indexOf(connectionsOfType.find((x) => x.Name === payload.name)),
                1
            );
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchData.fulfilled, (state, { payload }) => {
                const { connectionStringsDto, ongoingTasksDto } = payload;
                const { connections, urlParameters } = state;

                connections.raven = Object.values(connectionStringsDto.RavenConnectionStrings).map(
                    (ravenConnectionString) => ({
                        ...ravenConnectionString,
                        Type: "Raven",
                        UsedByTasks: ongoingTasksDto.OngoingTasks.filter(
                            (task: OngoingTaskRavenEtl) =>
                                task.TaskType === "RavenEtl" && task.ConnectionStringName === ravenConnectionString.Name
                        ).map((x) => ({
                            id: x.TaskId,
                            name: x.TaskName,
                        })),
                    })
                );

                state.loadStatus = "success";

                if (urlParameters.name && urlParameters.type) {
                    const foundConnection = getConnectionsOfType(connections, urlParameters.type).find(
                        (x) => x.Name === urlParameters.name
                    );

                    if (foundConnection) {
                        state.initialEditConnection = foundConnection;
                    }
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

const getConnectionsOfType = (connections: Connections, type: StudioEtlType) => {
    switch (type) {
        case "Raven":
            return connections.raven;
        // TODO others
        default:
            return null; // TODO
    }
};

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
    state: (store: RootState) => {
        const connections = store.connectionStrings.connections;

        return {
            loadStatus: store.connectionStrings.loadStatus,
            connections: store.connectionStrings.connections,
            initialEditConnection: store.connectionStrings.initialEditConnection,
            isEmpty: connections.raven.length === 0,
        };
    },
};

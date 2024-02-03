import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";

export interface ClusterNode {
    nodeTag: string;
    serverUrl: string;
}

interface ClusterState {
    localNodeTag: string;
    nodes: EntityState<ClusterNode, string>;
    clientVersion: string;
    serverVersion: serverBuildVersionDto;
    passive: boolean;
}

const clusterNodesAdapter = createEntityAdapter<ClusterNode, string>({
    selectId: (node) => node.nodeTag,
});

const nodesSelectors = clusterNodesAdapter.getSelectors();

const selectAllNodes = (store: RootState) => nodesSelectors.selectAll(store.cluster.nodes);
const selectAllNodeTags = (store: RootState) => nodesSelectors.selectIds(store.cluster.nodes);
const selectNodeByTag = (nodeTag: string) => (store: RootState) =>
    nodesSelectors.selectById(store.cluster.nodes, nodeTag);

const selectLocalNode = (store: RootState) =>
    nodesSelectors.selectById(store.cluster.nodes, store.cluster.localNodeTag);
const selectLocalNodeTag = (store: RootState) => store.cluster.localNodeTag;

const initialState: ClusterState = {
    localNodeTag: "A",
    nodes: clusterNodesAdapter.getInitialState(),
    clientVersion: null,
    serverVersion: null,
    passive: true,
};

export const clusterSlice = createSlice({
    initialState,
    name: "cluster",
    reducers: {
        serverStateLoaded: (state, action: PayloadAction<{ passive: boolean }>) => {
            state.passive = action.payload.passive;
        },
        nodesLoaded: (state, action: PayloadAction<ClusterNode[]>) => {
            const bootstrapped = !action.payload.some((x) => x.nodeTag === "?");
            clusterNodesAdapter.setAll(state.nodes, bootstrapped ? action.payload : []);
        },
        localNodeTagLoaded: (state, action: PayloadAction<string>) => {
            state.localNodeTag = action.payload;
        },
        clientVersionLoaded: (state, { payload: version }: PayloadAction<string>) => {
            state.clientVersion = version;
        },
        serverVersionLoaded: (state, { payload: version }: PayloadAction<serverBuildVersionDto>) => {
            state.serverVersion = version;
        },
    },
});

export const clusterActions = clusterSlice.actions;

export const clusterSelectors = {
    allNodes: selectAllNodes,
    allNodeTags: selectAllNodeTags,
    nodeByTag: selectNodeByTag,
    localNode: selectLocalNode,
    localNodeTag: selectLocalNodeTag,
    clientVersion: (store: RootState) => store.cluster.clientVersion,
    serverVersion: (store: RootState) => store.cluster.serverVersion,
    isPassive: (store: RootState) => store.cluster.passive,
};

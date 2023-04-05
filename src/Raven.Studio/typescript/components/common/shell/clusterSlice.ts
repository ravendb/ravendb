import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";

interface ClusterState {
    localNodeTag: string;

    nodeTags: string[];
}

const sliceName = "cluster";

const initialState: ClusterState = {
    localNodeTag: "A",
    nodeTags: [],
};

export const selectClusterNodeTags = (store: RootState) => store.cluster.nodeTags;
export const selectLocalNodeTag = (store: RootState) => store.cluster.localNodeTag;

export const clusterSlice = createSlice({
    initialState,
    name: sliceName,
    reducers: {
        nodeTagsLoaded: (state, action: PayloadAction<string[]>) => {
            const bootstrapped = !action.payload.includes("?");
            state.nodeTags = bootstrapped ? action.payload : [];
        },
        localNodeTagLoaded: (state, action: PayloadAction<string>) => {
            state.localNodeTag = action.payload;
        },
    },
});

export const { nodeTagsLoaded, localNodeTagLoaded } = clusterSlice.actions;

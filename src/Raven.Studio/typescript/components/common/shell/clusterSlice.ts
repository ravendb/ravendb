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

const selectClusterNodeTags = (store: RootState) => store.cluster.nodeTags;
const selectLocalNodeTag = (store: RootState) => store.cluster.localNodeTag;

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


export const clusterSelectors = {
    clusterNodeTags: selectClusterNodeTags,
    localNodeTag: selectLocalNodeTag
}

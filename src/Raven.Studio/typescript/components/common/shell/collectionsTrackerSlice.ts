import { EntityState, PayloadAction, createEntityAdapter, createSlice } from "@reduxjs/toolkit";
import { RootState } from "components/store";

const collectionNames = {
    allDocuments: "All Documents",
    revisionsBin: "Revisions Bin",
    hilo: "@hilo",
} as const;

interface Collection {
    name: (typeof collectionNames)[keyof typeof collectionNames] | (string & NonNullable<unknown>);
    documentCount: number;
    lastDocumentChangeVector: string;
    sizeClass: string;
    countPrefix: string;
    hasBounceClass: boolean;
}

interface CollectionsTrackerState {
    collections: EntityState<Collection>;
}

const collectionsAdapter = createEntityAdapter<Collection>({
    selectId: (collection) => collection.name,
});

const collectionsSelectors = collectionsAdapter.getSelectors();

const initialState: CollectionsTrackerState = {
    collections: collectionsAdapter.getInitialState(),
};

export const collectionsTrackerSlice = createSlice({
    initialState,
    name: "collectionsTracker",
    reducers: {
        collectionsLoaded: (state, { payload: collections }: PayloadAction<Collection[]>) => {
            collectionsAdapter.setAll(state.collections, collections);
        },
    },
});

export const collectionsTrackerActions = collectionsTrackerSlice.actions;

export const collectionsTrackerSelectors = {
    collections: (store: RootState) => collectionsSelectors.selectAll(store.collectionsTracker.collections),
    collectionNames: (store: RootState) =>
        collectionsSelectors
            .selectIds(store.collectionsTracker.collections)
            .filter((name) => name !== collectionNames.allDocuments) as string[],
};

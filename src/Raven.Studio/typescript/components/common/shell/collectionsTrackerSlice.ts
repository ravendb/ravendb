import { EntityState, PayloadAction, createEntityAdapter, createSelector, createSlice } from "@reduxjs/toolkit";
import { RootState } from "components/store";

const collectionNames = {
    allDocuments: "All Documents",
    allRevisions: "All Revisions",
    revisionsBin: "Revisions Bin",
    hilo: "@hilo",
    empty: "@empty",
} as const;

type CollectionName = (typeof collectionNames)[keyof typeof collectionNames] | (string & NonNullable<unknown>);

export interface Collection {
    name: CollectionName;
    documentCount: number;
    lastDocumentChangeVector: string;
    sizeClass: string;
    countPrefix: string;
    hasBounceClass: boolean;
}

interface CollectionsTrackerState {
    collections: EntityState<Collection, CollectionName>;
}

const collectionsAdapter = createEntityAdapter<Collection, CollectionName>({
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

const selectCollectionNames = createSelector(
    (store: RootState) => collectionsSelectors.selectIds(store.collectionsTracker.collections),
    (collections) => collections.filter((name) => name !== collectionNames.allDocuments)
);

export const collectionsTrackerSelectors = {
    collections: (store: RootState) => collectionsSelectors.selectAll(store.collectionsTracker.collections),
    collectionNames: selectCollectionNames,
};

import { EntityState, PayloadAction, createEntityAdapter, createSelector, createSlice } from "@reduxjs/toolkit";
import { RootState } from "components/store";

export const systemCollectionNames = {
    allDocuments: "All Documents",
    allRevisions: "All Revisions",
    revisionsBin: "Revisions Bin",
    hilo: "@hilo",
    empty: "@empty",
} as const;

type CollectionName =
    | (typeof systemCollectionNames)[keyof typeof systemCollectionNames]
    | (string & NonNullable<unknown>);

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
    globalChangeVector: string | null;
}

const collectionsAdapter = createEntityAdapter<Collection, CollectionName>({
    selectId: (collection) => collection.name,
});

const collectionsSelectors = collectionsAdapter.getSelectors();

const initialState: CollectionsTrackerState = {
    collections: collectionsAdapter.getInitialState(),
    globalChangeVector: null,
};

export const collectionsTrackerSlice = createSlice({
    initialState,
    name: "collectionsTracker",
    reducers: {
        collectionsLoaded: (state, { payload: collections }: PayloadAction<Collection[]>) => {
            collectionsAdapter.setAll(state.collections, collections);
        },
        globalChangeVectorUpdated: (state, { payload: changeVector }: PayloadAction<string | null>) => {
            state.globalChangeVector = changeVector;
        },
    },
});

export const collectionsTrackerActions = collectionsTrackerSlice.actions;

const selectCollectionNames = createSelector(
    (store: RootState) => collectionsSelectors.selectIds(store.collectionsTracker.collections),
    (collections) => collections.filter((name) => name !== systemCollectionNames.allDocuments)
);

const selectUserCollectionNames = createSelector(selectCollectionNames, (collections) =>
    collections.filter((name) => !String(name).startsWith("@"))
);

export const collectionsTrackerSelectors = {
    collections: (store: RootState) => collectionsSelectors.selectAll(store.collectionsTracker.collections),
    collectionByName: (name: CollectionName) => (store: RootState) =>
        collectionsSelectors.selectById(store.collectionsTracker.collections, name) ?? null,
    collectionNames: selectCollectionNames,
    userCollectionNames: selectUserCollectionNames,
    globalChangeVector: (store: RootState) => store.collectionsTracker.globalChangeVector,
};

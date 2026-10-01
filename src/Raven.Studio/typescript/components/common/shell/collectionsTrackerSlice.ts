import { EntityState, PayloadAction, createEntityAdapter, createSelector, createSlice } from "@reduxjs/toolkit";
import { shallowEqual } from "react-redux";
import { RootState } from "components/store";
import { StringWithAutocomplete } from "components/utils/common";

export const systemCollectionNames = {
    allDocuments: "All Documents",
    allRevisions: "All Revisions",
    revisionsBin: "Revisions Bin",
    hilo: "@hilo",
    empty: "@empty",
} as const;

type CollectionName = StringWithAutocomplete<(typeof systemCollectionNames)[keyof typeof systemCollectionNames]>;

export interface Collection {
    name: CollectionName;
    documentCount: number;
    lastDocumentChangeVector: string;
    sizeClass: string;
    countPrefix: string;
    hasBounceClass: boolean;
}

interface CollectionsTrackerState {
    databaseName: string | null;
    collections: EntityState<Collection, CollectionName>;
    globalChangeVector: string | null;
}

interface CollectionsLoadedPayload {
    databaseName: string | null;
    collections: Collection[];
}

const collectionsAdapter = createEntityAdapter<Collection, CollectionName>({
    selectId: (collection) => collection.name,
});

const collectionsSelectors = collectionsAdapter.getSelectors();

const initialState: CollectionsTrackerState = {
    databaseName: null,
    collections: collectionsAdapter.getInitialState(),
    globalChangeVector: null,
};

export const collectionsTrackerSlice = createSlice({
    initialState,
    name: "collectionsTracker",
    reducers: {
        collectionsLoaded: (state, { payload }: PayloadAction<CollectionsLoadedPayload>) => {
            state.databaseName = payload.databaseName;
            collectionsAdapter.setAll(state.collections, payload.collections);
        },
        globalChangeVectorUpdated: (state, { payload: changeVector }: PayloadAction<string | null>) => {
            state.globalChangeVector = changeVector;
        },
    },
});

export const collectionsTrackerActions = collectionsTrackerSlice.actions;

const selectCollectionNames = createSelector(
    (store: RootState) => collectionsSelectors.selectIds(store.collectionsTracker.collections),
    (collections) => collections.filter((name) => name !== systemCollectionNames.allDocuments),
    { memoizeOptions: { resultEqualityCheck: shallowEqual } }
);

const selectUserCollectionNames = createSelector(selectCollectionNames, (collections) =>
    collections.filter((name) => !String(name).startsWith("@"))
);

export const collectionsTrackerSelectors = {
    databaseName: (store: RootState) => store.collectionsTracker.databaseName,
    collections: (store: RootState) => collectionsSelectors.selectAll(store.collectionsTracker.collections),
    collectionByName: (name: CollectionName) => (store: RootState) =>
        collectionsSelectors.selectById(store.collectionsTracker.collections, name) ?? null,
    collectionNames: selectCollectionNames,
    userCollectionNames: selectUserCollectionNames,
    globalChangeVector: (store: RootState) => store.collectionsTracker.globalChangeVector,
};

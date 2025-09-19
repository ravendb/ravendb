import { RootState } from "components/store";
import { createSelector } from "@reduxjs/toolkit";
import { documentSchemaSliceInternal } from "./documentSchemaSlice";

const { validatorsSelectors } = documentSchemaSliceInternal;

const allValidators = (store: RootState) => validatorsSelectors.selectAll(store.documentSchema.validators);

const allCollectionNames = (store: RootState) =>
    validatorsSelectors.selectIds(store.documentSchema.validators) as string[];

const selectedCollectionNames = (store: RootState) => store.documentSchema.selectedCollectionNames;

const isSelectedCollectionName = (name: string) => (store: RootState) =>
    store.documentSchema.selectedCollectionNames.includes(name);

const isAnyModified = (store: RootState) =>
    !_.isEqual(store.documentSchema.originalValidators, store.documentSchema.validators);

const validatorByName = (name: string) => (store: RootState) =>
    validatorsSelectors.selectById(store.documentSchema.validators, name);

const validatorsCount = createSelector(allValidators, (items) => items.length);

const newDraftIds = (store: RootState) => store.documentSchema.newDraftIds;
const newDraftsCount = createSelector(newDraftIds, (ids) => ids.length);

export const documentSchemaSelectors = {
    allValidators,
    allCollectionNames,
    selectedCollectionNames,
    isSelectedCollectionName,
    isAnyModified,
    validatorByName,
    validatorsCount,
    newDraftIds,
    newDraftsCount,
};

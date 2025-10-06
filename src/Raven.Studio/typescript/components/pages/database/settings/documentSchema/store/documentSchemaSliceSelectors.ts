import { RootState } from "components/store";
import { documentSchemaSliceInternal } from "./documentSchemaSlice";

const { validatorsSelectors } = documentSchemaSliceInternal;

const allValidators = (store: RootState) => validatorsSelectors.selectAll(store.documentSchema.validators);

const allCollectionNames = (store: RootState) => validatorsSelectors.selectIds(store.documentSchema.validators);

const selectedCollectionNames = (store: RootState) => store.documentSchema.selectedCollectionNames;

const isSelectedCollectionName = (name: string) => (store: RootState) =>
    store.documentSchema.selectedCollectionNames.includes(name);

const newDraftIds = (store: RootState) => store.documentSchema.newDraftIds;

const globalDisabled = (store: RootState) => store.documentSchema.globalDisabled;

export const documentSchemaSelectors = {
    allValidators,
    allCollectionNames,
    selectedCollectionNames,
    isSelectedCollectionName,
    newDraftIds,
    globalDisabled,
};

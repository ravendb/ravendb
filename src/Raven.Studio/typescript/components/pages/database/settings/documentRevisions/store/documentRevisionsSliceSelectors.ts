import { RootState } from "components/store";
import {
    documentRevisionsConfigNames,
    DocumentRevisionsConfigName,
    documentRevisionsSliceInternal,
} from "./documentRevisionsSlice";

const { configsSelectors } = documentRevisionsSliceInternal;

const loadStatus = (store: RootState) => store.documentRevisions.loadStatus;

const defaultDocumentsConfig = (store: RootState) =>
    configsSelectors.selectById(store.documentRevisions.configs, documentRevisionsConfigNames.defaultDocument);

const defaultConflictsConfig = (store: RootState) =>
    configsSelectors.selectById(store.documentRevisions.configs, documentRevisionsConfigNames.defaultConflicts);

const collectionConfigs = (store: RootState) =>
    configsSelectors
        .selectAll(store.documentRevisions.configs)
        .filter(
            (x) =>
                x.Name !== documentRevisionsConfigNames.defaultConflicts &&
                x.Name !== documentRevisionsConfigNames.defaultDocument
        );

const allConfigsNames = (store: RootState) =>
    configsSelectors.selectIds(store.documentRevisions.configs) as DocumentRevisionsConfigName[];

const selectedConfigNames = (store: RootState) => store.documentRevisions.selectedConfigNames;

const isSelectedConfigName = (name: DocumentRevisionsConfigName) => (store: RootState) =>
    store.documentRevisions.selectedConfigNames.includes(name);

const isAnyModified = (store: RootState) =>
    !_.isEqual(store.documentRevisions.originalConfigs, store.documentRevisions.configs);

const originalConfig = (name: string) => (store: RootState) =>
    configsSelectors.selectById(store.documentRevisions.originalConfigs, name);

export const documentRevisionsSelectors = {
    loadStatus,
    defaultDocumentsConfig,
    defaultConflictsConfig,
    collectionConfigs,
    allConfigsNames,
    selectedConfigNames,
    isSelectedConfigName,
    isAnyModified,
    originalConfig,
};

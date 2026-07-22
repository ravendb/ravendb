import { create } from "zustand";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { getAncestorTablePaths, type MapActiveTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

export type ImportState = "none" | "locked" | "unlocked";

export type SetupWizardState = {
    reset: () => void;
    discoverResult: DiscoverResponse | null;
    discoverSchemas: string[];
    setDiscoverResult: (result: DiscoverResponse, discoverSchemas: string[]) => void;
    importState: ImportState;
    lockImportedConfig: () => void;
    unlockImportedConfig: () => void;
    connectKey: string | null;
    setConnectKey: (key: string) => void;
    appliedMapKey: string | null;
    setAppliedMapKey: (key: string) => void;
    mapTablesKey: string | null;
    setMapTablesKey: (key: string) => void;
    invalidateMapping: () => void;
    mapActiveTable: MapActiveTable | null;
    setMapActiveTable: (table: MapActiveTable | null) => void;
    /** Bumped by focusMapTable so the explorer scrolls the focused table into view. */
    mapFocusRequestId: number;
    focusMapTable: (table: MapActiveTable) => void;
    mapTablesFilter: string;
    setMapTablesFilter: (filter: string) => void;
    mapExpandedPaths: Record<string, boolean>;
    toggleMapTableExpanded: (path: string) => void;
    expandMapTable: (path: string) => void;
    setAllMapTablesExpanded: (paths: Record<string, boolean>) => void;
    removeMapTableUiState: (path: string) => void;
    resetMapTablesUiState: () => void;
    isMapTablesRawView: boolean;
    mapTablesRawContent: string;
    openMapTablesRawView: (content: string) => void;
    closeMapTablesRawView: () => void;
    setMapTablesRawContent: (content: string) => void;
};

const initialState: Pick<
    SetupWizardState,
    | "discoverResult"
    | "discoverSchemas"
    | "importState"
    | "connectKey"
    | "appliedMapKey"
    | "mapTablesKey"
    | "mapActiveTable"
    | "mapFocusRequestId"
    | "mapTablesFilter"
    | "mapExpandedPaths"
    | "isMapTablesRawView"
    | "mapTablesRawContent"
> = {
    discoverResult: null,
    discoverSchemas: [],
    importState: "none",
    connectKey: null,
    appliedMapKey: null,
    mapTablesKey: null,
    mapActiveTable: null,
    mapFocusRequestId: 0,
    mapTablesFilter: "",
    mapExpandedPaths: {},
    isMapTablesRawView: false,
    mapTablesRawContent: "",
};

export const useSetupWizardStore = create<SetupWizardState>((set) => ({
    ...initialState,
    reset: () => set(initialState),
    setDiscoverResult: (result, discoverSchemas) =>
        set({
            discoverResult: result,
            discoverSchemas,
        }),
    lockImportedConfig: () => set({ importState: "locked" }),
    unlockImportedConfig: () => set({ importState: "unlocked" }),
    setConnectKey: (key) => set({ connectKey: key }),
    setAppliedMapKey: (key) => set({ appliedMapKey: key }),
    setMapTablesKey: (key) => set({ mapTablesKey: key }),
    invalidateMapping: () => set({ appliedMapKey: null }),
    setMapActiveTable: (table) => set({ mapActiveTable: table }),
    focusMapTable: (table) =>
        set((state) => ({
            mapActiveTable: table,
            mapExpandedPaths: {
                ...state.mapExpandedPaths,
                ...Object.fromEntries(getAncestorTablePaths(table.path).map((path) => [path, true])),
            },
            mapFocusRequestId: state.mapFocusRequestId + 1,
        })),
    setMapTablesFilter: (filter) => set({ mapTablesFilter: filter }),
    toggleMapTableExpanded: (path) =>
        set((state) => ({
            mapExpandedPaths: { ...state.mapExpandedPaths, [path]: !state.mapExpandedPaths[path] },
        })),
    expandMapTable: (path) =>
        set((state) => ({
            mapExpandedPaths: { ...state.mapExpandedPaths, [path]: true },
        })),
    setAllMapTablesExpanded: (paths) => set({ mapExpandedPaths: paths }),
    removeMapTableUiState: (path) =>
        set((state) => ({
            mapActiveTable: null,
            mapExpandedPaths: Object.fromEntries(
                Object.entries(state.mapExpandedPaths).filter(
                    ([expandedPath]) => expandedPath !== path && !expandedPath.startsWith(`${path}.`),
                ),
            ),
        })),
    resetMapTablesUiState: () =>
        set({
            mapActiveTable: null,
            mapTablesFilter: "",
            mapExpandedPaths: {},
            isMapTablesRawView: false,
            mapTablesRawContent: "",
        }),
    openMapTablesRawView: (content) => set({ isMapTablesRawView: true, mapTablesRawContent: content }),
    closeMapTablesRawView: () => set({ isMapTablesRawView: false, mapTablesRawContent: "" }),
    setMapTablesRawContent: (content) => set({ mapTablesRawContent: content }),
}));

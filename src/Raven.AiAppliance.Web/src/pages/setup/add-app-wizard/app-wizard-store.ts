import { create } from "zustand";
import type { DiscoverResponse } from "@/api/generated/server-api";
import type { MapActiveTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

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
    mapExpandedPaths: Record<string, boolean>;
    toggleMapTableExpanded: (path: string) => void;
    expandMapTable: (path: string) => void;
    setAllMapTablesExpanded: (paths: Record<string, boolean>) => void;
    removeMapTableUiState: (path: string) => void;
    resetMapTablesUiState: () => void;
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
    | "mapExpandedPaths"
> = {
    discoverResult: null,
    discoverSchemas: [],
    importState: "none",
    connectKey: null,
    appliedMapKey: null,
    mapTablesKey: null,
    mapActiveTable: null,
    mapExpandedPaths: {},
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
    resetMapTablesUiState: () => set({ mapActiveTable: null, mapExpandedPaths: {} }),
}));

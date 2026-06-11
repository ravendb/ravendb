import { create } from "zustand";
import type { DiscoverResponse } from "@/api/generated/server-api";
import type { MapActiveTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

export type SetupWizardState = {
    reset: () => void;
    discoverResult: DiscoverResponse | null;
    setDiscoverResult: (x: DiscoverResponse) => void;
    /** Fingerprint of the inputs used to generate mapTables.tables, so re-entering the map step keeps user edits. */
    appliedMapKey: string | null;
    setAppliedMapKey: (key: string) => void;
    mapActiveTable: MapActiveTable | null;
    setMapActiveTable: (table: MapActiveTable | null) => void;
    mapExpandedPaths: Record<string, boolean>;
    toggleMapTableExpanded: (path: string) => void;
    expandMapTable: (path: string) => void;
    setAllMapTablesExpanded: (paths: Record<string, boolean>) => void;
    removeMapTableUiState: (path: string) => void;
    resetMapTablesUiState: () => void;
};

const initialState: Pick<SetupWizardState, "discoverResult" | "appliedMapKey" | "mapActiveTable" | "mapExpandedPaths"> =
    {
        discoverResult: null,
        appliedMapKey: null,
        mapActiveTable: null,
        mapExpandedPaths: {},
    };

export const useSetupWizardStore = create<SetupWizardState>((set) => ({
    ...initialState,
    reset: () => set(initialState),
    setDiscoverResult: (result) =>
        set({
            discoverResult: result,
        }),
    setAppliedMapKey: (key) => set({ appliedMapKey: key }),
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

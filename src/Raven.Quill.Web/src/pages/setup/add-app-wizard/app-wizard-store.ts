import { create } from "zustand";
import type { DiscoverResponse } from "@/api/generated/server-api";
import {
    getAncestorTablePaths,
    type MapActiveTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

/**
 * Protects a mapping the wizard started from - an imported file, or the edited app's own
 * configuration. While locked, the inputs it was generated from stay frozen.
 */
export type ConfigLockState = "none" | "locked" | "unlocked";

/** Outcome of a connect attempt together with the connect key it ran with. */
export type ConnectionAttempt = { key: string; error: Error | null };

export type SetupWizardState = {
    reset: () => void;
    discoverResult: DiscoverResponse | null;
    discoverSchemas: string[];
    setDiscoverResult: (result: DiscoverResponse, discoverSchemas: string[]) => void;
    /** Slug of the app being edited; null while a new app is being created. */
    editedAppSlug: string | null;
    startEditingApp: (slug: string, discoverSchemas: string[]) => void;
    configLock: ConfigLockState;
    lockConfig: () => void;
    unlockConfig: () => void;
    /**
     * Last connect attempt made via "Test connection" (or a previous Next). Both the verified state and
     * the failure alert are derived from it, so neither survives an edit to the connection inputs.
     */
    connectionAttempt: ConnectionAttempt | null;
    setConnectionAttempt: (attempt: ConnectionAttempt) => void;
    /**
     * The keys below record what the wizard already did for a given set of inputs, so a step can skip
     * work when nothing it depends on changed. Everything the server keeps per app - the discovery, the
     * CDC dry run, the stored map configuration - is keyed by connectKey, because that is the state
     * document those calls wrote into. The mapping the operator edits in the form is keyed by the source
     * alone, so renaming the app keeps it.
     */
    connectKey: string | null;
    setConnectKey: (key: string) => void;
    appliedMapKey: string | null;
    setAppliedMapKey: (key: string) => void;
    mapTablesKey: string | null;
    setMapTablesKey: (key: string) => void;
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
    | "editedAppSlug"
    | "configLock"
    | "connectKey"
    | "connectionAttempt"
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
    editedAppSlug: null,
    configLock: "none",
    connectKey: null,
    connectionAttempt: null,
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
    // Starts locked: the mapping comes from the app itself. Its schemas are seeded too - discovery
    // only covers the default one otherwise, and tables it misses cannot be verified.
    startEditingApp: (slug, discoverSchemas) => set({ editedAppSlug: slug, configLock: "locked", discoverSchemas }),
    lockConfig: () => set({ configLock: "locked" }),
    unlockConfig: () => set({ configLock: "unlocked" }),
    setConnectKey: (key) => set({ connectKey: key }),
    setConnectionAttempt: (attempt) => set({ connectionAttempt: attempt }),
    setAppliedMapKey: (key) => set({ appliedMapKey: key }),
    setMapTablesKey: (key) => set({ mapTablesKey: key }),
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

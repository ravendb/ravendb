import { create } from "zustand";
import type { MigrationFrame } from "@/api/custom-services/migration-service";
import type { DiscoverResponse } from "@/api/generated/server-api";
import {
    getAncestorTablePaths,
    type MapActiveTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import type { PlannerQuestion } from "@/pages/setup/add-app-wizard/steps/map/planner-questions-utils";

/** Outcome of a connect attempt together with the connect key it ran with. */
export type ConnectionAttempt = { key: string; error: Error | null };

/** Identity of a selected source table, as the verify step's form rows carry it. */
export type SelectedSourceTable = { sourceTableSchema: string | null; sourceTableName: string };

export type SetupWizardState = {
    reset: () => void;
    discoverResult: DiscoverResponse | null;
    discoverSchemas: string[];
    setDiscoverResult: (result: DiscoverResponse, discoverSchemas: string[]) => void;
    /** Slug of the app being edited; null while a new app is being created. */
    editedAppSlug: string | null;
    hasEditedSlug: boolean;
    setHasEditedSlug: (hasEdited: boolean) => void;
    startEditingApp: (slug: string, discoverSchemas: string[], initialSelectedTables: SelectedSourceTable[]) => void;
    /**
     * Table selection the wizard started from - the edited app's own configuration, or an imported
     * file. Null while a new app is built from scratch. The verify step's stepper badge compares
     * against it to flag a changed selection.
     */
    initialSelectedTables: SelectedSourceTable[] | null;
    setInitialSelectedTables: (tables: SelectedSourceTable[]) => void;
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
    /** Whether the editor's content still parses into tables. Recorded by the writer, which parses
     * anyway, so nothing has to re-parse a whole mapping to answer it. */
    isMapTablesRawContentValid: boolean;
    openMapTablesRawView: (content: string) => void;
    closeMapTablesRawView: () => void;
    setMapTablesRawContent: (content: string, isValid: boolean) => void;

    /**
     * The planner session. It lives here rather than in the step body because the wizard remounts
     * the body on every step change - holding the conversation id in component state would mean
     * leaving the step and coming back could only start over, never resume.
     */
    plannerMessages: PlannerMessage[];
    plannerConversationId: string | null;
    /** Registered collections, keyed by name so a re-emit replaces rather than appends. */
    plannerCollections: Record<string, PlannerCollection>;
    plannerProposal: PlannerProposal | null;
    /** Names the operator kept. Absent from the map means kept - a new collection starts selected. */
    plannerDeselected: Record<string, boolean>;
    isPlannerStreaming: boolean;
    /** Questions the operator has to answer, or skip, before the session can go on. */
    plannerQuestions: PlannerQuestion[];
    /** The plan and selection last handed to the editor, so going Back and Next again does not
     * re-apply an unchanged plan over edits made in the editor since. */
    plannerAppliedKey: string | null;
    appendPlannerMessage: (message: PlannerMessage) => void;
    setPlannerConversationId: (conversationId: string) => void;
    setPlannerProposal: (proposal: PlannerProposal) => void;
    upsertPlannerCollection: (collection: PlannerCollection) => void;
    removePlannerCollection: (collection: string) => void;
    togglePlannerCollection: (collection: string, isSelected: boolean) => void;
    setIsPlannerStreaming: (isStreaming: boolean) => void;
    setPlannerQuestions: (questions: PlannerQuestion[]) => void;
    setPlannerAppliedKey: (key: string) => void;
    resetPlannerState: () => void;
};

/** One turn in the planner transcript. Tool activity is a marker line, not a bubble. */
export type PlannerMessage = {
    id: string;
    role: "user" | "agent" | "tool" | "error";
    text: string;
};

export type PlannerProposal = Omit<Extract<MigrationFrame, { type: "proposal" }>, "type">;

export type PlannerCollection = {
    collection: string;
    version: number;
    status: string;
    rationale?: string | null;
    config?: unknown;
    warnings: string[];
    /** Set when the last attempt was rejected; the card shows why instead of a mapping. */
    errors?: string[];
};

const initialState: Pick<
    SetupWizardState,
    | "discoverResult"
    | "discoverSchemas"
    | "editedAppSlug"
    | "hasEditedSlug"
    | "initialSelectedTables"
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
    | "isMapTablesRawContentValid"
    | "plannerMessages"
    | "plannerConversationId"
    | "plannerCollections"
    | "plannerProposal"
    | "plannerDeselected"
    | "isPlannerStreaming"
    | "plannerQuestions"
    | "plannerAppliedKey"
> = {
    discoverResult: null,
    discoverSchemas: [],
    editedAppSlug: null,
    hasEditedSlug: false,
    initialSelectedTables: null,
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
    isMapTablesRawContentValid: true,
    plannerMessages: [],
    plannerConversationId: null,
    plannerCollections: {},
    plannerProposal: null,
    plannerDeselected: {},
    isPlannerStreaming: false,
    plannerQuestions: [],
    plannerAppliedKey: null,
};

export const useSetupWizardStore = create<SetupWizardState>((set) => ({
    ...initialState,
    reset: () => set(initialState),
    setDiscoverResult: (result, discoverSchemas) =>
        set({
            discoverResult: result,
            discoverSchemas,
        }),
    // The app's schemas are seeded too - discovery only covers the default one otherwise, and
    // tables it misses cannot be verified.
    startEditingApp: (slug, discoverSchemas, initialSelectedTables) =>
        set({ editedAppSlug: slug, discoverSchemas, initialSelectedTables }),
    setHasEditedSlug: (hasEditedSlug) => set({ hasEditedSlug }),
    setInitialSelectedTables: (tables) => set({ initialSelectedTables: tables }),
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
            isMapTablesRawContentValid: true,
        }),
    // The content is serialized from the form, so it parses by construction.
    openMapTablesRawView: (content) =>
        set({ isMapTablesRawView: true, mapTablesRawContent: content, isMapTablesRawContentValid: true }),
    closeMapTablesRawView: () =>
        set({ isMapTablesRawView: false, mapTablesRawContent: "", isMapTablesRawContentValid: true }),
    setMapTablesRawContent: (content, isValid) =>
        set({ mapTablesRawContent: content, isMapTablesRawContentValid: isValid }),

    appendPlannerMessage: (message) => set((state) => ({ plannerMessages: [...state.plannerMessages, message] })),
    setPlannerConversationId: (conversationId) => set({ plannerConversationId: conversationId }),
    setPlannerProposal: (proposal) => set({ plannerProposal: proposal }),

    // Keyed by name because add_collection is an upsert: a re-emit after a convention change
    // replaces the card rather than stacking a second one beside it.
    upsertPlannerCollection: (collection) =>
        set((state) => ({
            plannerCollections: { ...state.plannerCollections, [collection.collection]: collection },
        })),

    removePlannerCollection: (collection) =>
        set((state) => ({
            plannerCollections: Object.fromEntries(
                Object.entries(state.plannerCollections).filter(([name]) => name !== collection),
            ),
        })),

    togglePlannerCollection: (collection, isSelected) =>
        set((state) => ({
            plannerDeselected: { ...state.plannerDeselected, [collection]: !isSelected },
        })),

    setIsPlannerStreaming: (isStreaming) => set({ isPlannerStreaming: isStreaming }),

    setPlannerQuestions: (questions) => set({ plannerQuestions: questions }),

    setPlannerAppliedKey: (key) => set({ plannerAppliedKey: key }),

    resetPlannerState: () =>
        set({
            plannerMessages: [],
            plannerConversationId: null,
            plannerCollections: {},
            plannerProposal: null,
            plannerDeselected: {},
            isPlannerStreaming: false,
            plannerQuestions: [],
            plannerAppliedKey: null,
        }),
}));

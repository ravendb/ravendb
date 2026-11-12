import { BackupDecisionLogDetails } from "./backupDecisionLogTypes";
import { mergeDecisionLogEntries } from "./backupDecisionLogUtils";

export const backupDecisionLogTake = 512;

interface ActionConnecting {
    type: "Connecting";
}

interface ActionSnapshotLoaded {
    type: "SnapshotLoaded";
    details: BackupDecisionLogDetails;
}

interface ActionUpdateLoaded {
    type: "UpdateLoaded";
    details: BackupDecisionLogDetails;
}

interface ActionConnectionFailed {
    type: "ConnectionFailed";
}

type BackupDecisionLogReducerAction =
    | ActionConnecting
    | ActionSnapshotLoaded
    | ActionUpdateLoaded
    | ActionConnectionFailed;

export interface BackupDecisionLogState {
    details: BackupDecisionLogDetails | null;
    status: "loading" | "success" | "error";
}

export const backupDecisionLogInitialState: BackupDecisionLogState = {
    details: null,
    status: "loading",
};

export function backupDecisionLogReducer(
    state: BackupDecisionLogState,
    action: BackupDecisionLogReducerAction
): BackupDecisionLogState {
    switch (action.type) {
        case "Connecting":
            return { ...state, status: "loading" };
        case "SnapshotLoaded":
            return { details: action.details, status: "success" };
        case "UpdateLoaded":
            return {
                ...state,
                details: {
                    ...action.details,
                    Databases: state.details.Databases,
                    Tasks: state.details.Tasks,
                    Entries: mergeDecisionLogEntries(
                        state.details.Entries,
                        action.details.Entries,
                        backupDecisionLogTake
                    ),
                },
            };
        case "ConnectionFailed":
            // after the snapshot the client reconnects on its own and sends a new snapshot
            return state.status === "success" ? state : { ...state, status: "error" };
    }
}

import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import { backupDecisionLogInitialState, backupDecisionLogReducer } from "./backupDecisionLogReducer";

describe("backupDecisionLogReducer", () => {
    const details = ManageServerStubs.backupDecisionLog();
    const loaded = backupDecisionLogReducer(backupDecisionLogInitialState, { type: "SnapshotLoaded", details });

    it("shows an error when the connection fails before the snapshot", () => {
        const state = backupDecisionLogReducer(backupDecisionLogInitialState, { type: "ConnectionFailed" });

        expect(state.status).toBe("error");
    });

    it("keeps the loaded snapshot when the connection drops later", () => {
        const state = backupDecisionLogReducer(loaded, { type: "ConnectionFailed" });

        expect(state).toBe(loaded);
    });

    it("keeps the filter options of the snapshot when an update arrives", () => {
        const state = backupDecisionLogReducer(loaded, {
            type: "UpdateLoaded",
            details: { ...details, Databases: null, Tasks: null, Entries: [] },
        });

        expect(state.details.Databases).toEqual(details.Databases);
        expect(state.details.Tasks).toEqual(details.Tasks);
        expect(state.details.Entries).toHaveLength(details.Entries.length);
    });
});

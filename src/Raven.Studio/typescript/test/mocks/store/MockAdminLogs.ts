import { adminLogsActions } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { globalDispatch } from "components/storeCompat";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export class MockAdminLogs {
    with_logs(): void {
        globalDispatch(adminLogsActions.logsSet(ManageServerStubs.adminLogsMessages()));
    }
}

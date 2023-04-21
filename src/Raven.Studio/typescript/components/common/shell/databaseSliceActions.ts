import { DatabaseSharedInfo } from "components/models/databases";
import { AppAsyncThunk } from "components/store";
import deleteDatabaseConfirm from "viewmodels/resources/deleteDatabaseConfirm";
import app from "durandal/app";
import { databasesSlice } from "components/common/shell/databasesSlice";

//TODO: report success after database deletion? - what about other actions?
const confirmDeleteDatabases =
    (
        toDelete: DatabaseSharedInfo[]
    ): AppAsyncThunk<{ can: boolean; keepFiles?: boolean; databases?: DatabaseSharedInfo[] }> =>
    async (): Promise<{ can: boolean; keepFiles?: boolean; databases?: DatabaseSharedInfo[] }> => {
        const selectedDatabasesWithoutLock = toDelete.filter((x) => x.lockMode === "Unlock");
        if (selectedDatabasesWithoutLock.length === 0) {
            return {
                can: false,
            };
        }

        const confirmDeleteViewModel = new deleteDatabaseConfirm(selectedDatabasesWithoutLock);
        app.showBootstrapDialog(confirmDeleteViewModel);
        const baseResult = await confirmDeleteViewModel.result;
        return {
            ...baseResult,
            databases: selectedDatabasesWithoutLock,
        };
    };

const deleteDatabases =
    (toDelete: DatabaseSharedInfo[], keepFiles: boolean): AppAsyncThunk<updateDatabaseConfigurationsResult> =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();
        /* TODO:
                   const dbsList = toDelete.map(x => {
                       //TODO: x.isBeingDeleted(true);
                       const asDatabase = x.asDatabase();
        
                       // disconnect here to avoid race condition between database deleted message
                       // and websocket disconnection
                       //TODO: changesContext.default.disconnectIfCurrent(asDatabase, "DatabaseDeleted");
                       return asDatabase;
                   });*/

        return databasesService.deleteDatabase(
            toDelete.map((x) => x.name),
            !keepFiles
        );
    };

export const databaseActions = {
    activeDatabaseChanged: databasesSlice.actions.activeDatabaseChanged,
    databasesLoaded: databasesSlice.actions.databasesLoaded,
    confirmDeleteDatabases,
    deleteDatabases,
};

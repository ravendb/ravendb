import { createListenerMiddleware } from "@reduxjs/toolkit";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskStorageKeys,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";

export const editCdcSinkTaskMiddleware = createListenerMiddleware();

editCdcSinkTaskMiddleware.startListening({
    actionCreator: editCdcSinkTaskActions.fieldMappingExpandedByDefaultSet,
    effect: (action) => {
        localStorage.setItem(editCdcSinkTaskStorageKeys.isFieldMappingExpandedByDefault, String(action.payload));
    },
});

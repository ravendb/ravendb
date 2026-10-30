import messagePublisher from "common/messagePublisher";
import { Switch } from "components/common/Checkbox";
import {
    editCdcSinkTaskSelectors,
    editCdcSinkTaskActions,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { editCdcSinkTaskUtils } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskUtils";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { useFormContext } from "react-hook-form";
import { useEditCdcSinkTaskRawViewSync } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskRawViewSync";

interface EditCdcSinkTaskRawViewSwitchProps {
    taskId: number;
    isDisabled: boolean;
}

export default function EditCdcSinkTaskRawViewSwitch({ taskId, isDisabled }: EditCdcSinkTaskRawViewSwitchProps) {
    const dispatch = useAppDispatch();
    const isRawView = useAppSelector(editCdcSinkTaskSelectors.isRawView);
    const editForm = useFormContext<EditCdcSinkTaskFormData>();
    const { applyRawViewContent } = useEditCdcSinkTaskRawViewSync(editForm);

    const handleToggleRawView = (e: React.ChangeEvent<HTMLInputElement, Element>) => {
        if (e.target.checked) {
            try {
                const dto = editCdcSinkTaskUtils.mapToDto(editForm.getValues(), taskId);
                dispatch(editCdcSinkTaskActions.rawViewOpened(JSON.stringify(dto, null, 2)));
            } catch (error) {
                messagePublisher.reportError(
                    "The current form data cannot be converted. Please fix validation errors and try again.",
                    error
                );
            }
            return;
        }

        if (applyRawViewContent()) {
            dispatch(editCdcSinkTaskActions.rawViewClosed());
        }
    };

    return (
        <Switch selected={isRawView} toggleSelection={handleToggleRawView} color="primary" disabled={isDisabled}>
            Raw config
        </Switch>
    );
}

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

interface EditCdcSinkTaskRawViewSwitchProps {
    taskId: number;
}

export default function EditCdcSinkTaskRawViewSwitch({ taskId }: EditCdcSinkTaskRawViewSwitchProps) {
    const dispatch = useAppDispatch();
    const isRawView = useAppSelector(editCdcSinkTaskSelectors.isRawView);
    const rawViewContent = useAppSelector(editCdcSinkTaskSelectors.rawViewContent);

    const editForm = useFormContext<EditCdcSinkTaskFormData>();

    const handleToggleRawView = (e: React.ChangeEvent<HTMLInputElement, Element>) => {
        try {
            if (e.target.checked) {
                const currentFormData = editForm.getValues();
                const dto = editCdcSinkTaskUtils.mapToDto(currentFormData, taskId);
                const configAsString = JSON.stringify(dto, null, 2);
                dispatch(editCdcSinkTaskActions.rawViewContentSet(configAsString));
            } else {
                const parsedConfig = JSON.parse(rawViewContent);
                const formData = editCdcSinkTaskUtils.mapConfigFromDto(parsedConfig);
                editForm.reset(formData);
                dispatch(editCdcSinkTaskActions.rawViewContentSet(null));
            }

            dispatch(editCdcSinkTaskActions.rawViewToggled());
        } catch (error) {
            messagePublisher.reportError(
                "The current form data cannot be converted. Please fix validation errors and try again.",
                error
            );
        }
    };

    return (
        <Switch selected={isRawView} toggleSelection={handleToggleRawView} color="primary">
            Raw config
        </Switch>
    );
}

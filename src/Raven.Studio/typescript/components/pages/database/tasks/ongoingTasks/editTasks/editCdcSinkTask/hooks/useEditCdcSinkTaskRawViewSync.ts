import messagePublisher from "common/messagePublisher";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { editCdcSinkTaskUtils } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskUtils";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { UseFormReturn } from "react-hook-form";

export function useEditCdcSinkTaskRawViewSync(editForm: UseFormReturn<EditCdcSinkTaskFormData>) {
    const isRawView = useAppSelector(editCdcSinkTaskSelectors.isRawView);
    const rawViewContent = useAppSelector(editCdcSinkTaskSelectors.rawViewContent);

    const applyRawViewContent = (): boolean => {
        if (!isRawView) {
            return true;
        }

        try {
            const formData = editCdcSinkTaskUtils.mapConfigFromDto(JSON.parse(rawViewContent));
            editForm.reset(formData, { keepDefaultValues: true });
            return true;
        } catch (error) {
            messagePublisher.reportError(
                "The raw configuration cannot be converted. Please fix the JSON and try again.",
                error
            );
            return false;
        }
    };

    return applyRawViewContent;
}

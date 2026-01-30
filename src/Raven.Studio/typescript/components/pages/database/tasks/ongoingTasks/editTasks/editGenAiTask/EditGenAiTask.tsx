import "./EditGenAiTask.scss";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import { useServices } from "components/hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { tryHandleSubmit } from "components/utils/common";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "./store/editGenAiTaskSlice";
import { useEffect } from "react";
import { useEditGenAiTaskSteps } from "./hooks/useEditGenAiTaskSteps";
import {
    EditGenAiTaskFormData,
    editGenAiTaskResolver,
    EditGenAiTaskValidationContext,
} from "./utils/editGenAiTaskValidation";
import { editGenAiTaskUtils } from "./utils/editGenAiTaskUtils";
import EditGenAiTaskTestResults from "./partials/EditGenAiTaskTestResults";
import EditGenAiTaskSteps from "./partials/EditGenAiTaskSteps";
import EditGenAiTaskPlayground from "./partials/EditGenAiTaskPlayground";
import useEditGenAiCancel from "./hooks/useEditGenAiCancel";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { connectionStringsActions } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { defaultItemsToProcess } from "components/pages/database/settings/documentExpiration/DocumentExpiration";
import { TimeInSeconds } from "common/constants/timeInSeconds";
import { licenseSelectors } from "components/common/shell/licenseSlice";

interface QueryParams {
    taskId: string;
    sourceView: EditAiTaskSourceView;
}

export default function EditGenAiTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();

    const { tasksService, databasesService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isTestOpen = useAppSelector(editGenAiTaskSelectors.isTestOpen);
    const isDocumentExpirationEnabled = useAppSelector(editGenAiTaskSelectors.isDocumentExpirationEnabled);
    const isCommunityLicense = useAppSelector(licenseSelectors.licenseType) === "Community";

    const taskId = queryParams?.taskId ? parseInt(queryParams.taskId) : null;

    // Set query params to store
    useEffect(() => {
        if (taskId) {
            dispatch(editGenAiTaskActions.taskIdSet(taskId));
            dispatch(editGenAiTaskActions.currentStepSet("summary"));
        }

        if (queryParams) {
            dispatch(editGenAiTaskActions.sourceViewSet(queryParams.sourceView));
        }

        dispatch(editGenAiTaskActions.getIsDocumentExpirationEnabled(databaseName));

        dispatch(connectionStringsActions.viewContextSet("aiTask"));

        return () => {
            dispatch(editGenAiTaskActions.reset());
        };
    }, []);

    const cancel = useEditGenAiCancel();

    const getDefaultValues = async () => {
        if (taskId) {
            try {
                const dto = await tasksService.getGenAiTaskInfo(databaseName, taskId);
                return editGenAiTaskUtils.getDefaultValues(dto);
            } catch {
                cancel();
            }
        }

        return editGenAiTaskUtils.getDefaultValues(null);
    };

    const form = useForm<EditGenAiTaskFormData>({
        mode: "all",
        resolver: (data, _, options) =>
            editGenAiTaskResolver(
                data,
                {
                    allQueryNames: data.queries?.map((x) => x.name) ?? [],
                } satisfies EditGenAiTaskValidationContext,
                options
            ),
        defaultValues: getDefaultValues,
    });

    const { setIsDirty } = useDirtyFlag(form.formState.isDirty);

    const { handleSubmit, reset } = form;

    const handleSave: SubmitHandler<EditGenAiTaskFormData> = (data) => {
        return tryHandleSubmit(async () => {
            if (!isDocumentExpirationEnabled && data.isSetTracingExpiration) {
                await databasesService.saveExpirationConfiguration(databaseName, {
                    Disabled: false,
                    DeleteFrequencyInSec: isCommunityLicense ? TimeInSeconds.Day * 36 : null,
                    MaxItemsToProcess: defaultItemsToProcess,
                });
            }

            const scriptsToReset = data.isResetScript ? [data.scriptToReset] : undefined;
            await tasksService.saveGenAiTask(
                databaseName,
                editGenAiTaskUtils.mapToDto(data, taskId),
                scriptsToReset,
                editGenAiTaskUtils.getSerializedChangeVector(data, taskId)
            );

            reset(data);
            setIsDirty(false);
            cancel();
        });
    };

    const steps = useEditGenAiTaskSteps();
    const currentStep = steps.find((x) => x.isCurrent);
    const stepName = currentStep.id;

    const isPlaygroundVisible = stepName === "context" || stepName === "modelInput" || stepName === "updateScript";

    return (
        <FormProvider {...form}>
            <form onSubmit={handleSubmit(handleSave)} className="edit-gen-ai-task">
                <div className="main-container wizard-content">
                    {currentStep.component}
                    {isPlaygroundVisible && <EditGenAiTaskPlayground />}
                </div>
                <div className="footer">
                    <div className="footer-content">{currentStep.footer}</div>
                </div>
                <div className="sidebar wizard-sidebar">
                    {isTestOpen ? <EditGenAiTaskTestResults /> : <EditGenAiTaskSteps steps={steps} />}
                </div>
            </form>
        </FormProvider>
    );
}

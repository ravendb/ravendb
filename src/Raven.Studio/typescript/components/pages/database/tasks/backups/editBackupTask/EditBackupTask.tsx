import "./EditBackupTask.scss";
import { useState } from "react";
import { useAsync } from "react-async-hook";
import { FormProvider, SubmitErrorHandler, SubmitHandler, useForm } from "react-hook-form";
import router from "plugins/router";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { useServices } from "components/hooks/useServices";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { tryHandleSubmit } from "components/utils/common";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import RichAlert from "components/common/RichAlert";
import WizardLayout from "components/common/wizard/WizardLayout";
import WizardStepsList from "components/common/wizard/WizardStepsList";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import notificationCenter from "common/notifications/notificationCenter";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import {
    CronServerErrors,
    EditBackupTaskFormData,
    editBackupTaskYupResolver,
    EditBackupTaskValidationContext,
} from "./utils/editBackupTaskValidation";
import { editBackupTaskUtils } from "./utils/editBackupTaskUtils";
import {
    EditBackupTaskContext,
    EditBackupTaskStepId,
    getEditBackupTaskStepFields,
    getEditBackupTaskSteps,
} from "./utils/editBackupTaskSteps";
import EditBackupTaskFooter from "./partials/EditBackupTaskFooter";
import EditBackupTaskHeader from "./partials/EditBackupTaskHeader";
import EditBackupTaskStepType from "./steps/EditBackupTaskStepType";
import EditBackupTaskStepSettings from "./steps/EditBackupTaskStepSettings";
import EditBackupTaskStepSchedule from "./steps/EditBackupTaskStepSchedule";
import EditBackupTaskStepDestinations from "./steps/EditBackupTaskStepDestinations";
import EditBackupTaskStepSummary from "./steps/EditBackupTaskStepSummary";
import assertUnreachable from "components/utils/assertUnreachable";

interface QueryParams {
    taskId?: string;
    manual?: string;
    sourceView?: EditPeriodicBackupTaskSourceView;
}

export default function EditBackupTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { tasksService } = useServices();
    const t = useStudioTranslation("editBackupTask");

    const taskId = queryParams?.taskId ? parseInt(queryParams.taskId, 10) : null;
    const isManual = !taskId && queryParams?.manual === "true";

    const asyncLoad = useAsync(async () => {
        const [serverLimits, dto] = await Promise.all([
            hasDatabaseAdminAccess
                ? tasksService.getPeriodicBackupServerLimits(databaseName)
                : editBackupTaskUtils.UNKNOWN_SERVER_LIMITS,
            taskId ? tasksService.getPeriodicBackupConfiguration(databaseName, taskId) : null,
        ]);
        return { serverLimits, dto };
    }, [databaseName, taskId, hasDatabaseAdminAccess]);

    if (asyncLoad.loading) {
        return <LoadingView />;
    }

    if (asyncLoad.error) {
        return <LoadError error={t("loadError")} refresh={asyncLoad.execute} />;
    }

    return (
        <EditBackupTaskForm
            dto={asyncLoad.result.dto}
            serverLimits={asyncLoad.result.serverLimits}
            taskId={taskId}
            isManual={isManual}
            sourceView={queryParams?.sourceView ?? "Backups"}
        />
    );
}

interface EditBackupTaskFormProps {
    dto: Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration | null;
    serverLimits: periodicBackupServerLimitsResponse;
    taskId: number | null;
    isManual: boolean;
    sourceView: EditPeriodicBackupTaskSourceView;
}

function EditBackupTaskForm({ dto, serverLimits, taskId, isManual, sourceView }: EditBackupTaskFormProps) {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { tasksService } = useServices();
    const { appUrl } = useAppUrls();
    const { reportEvent } = useEventsCollector();
    const t = useStudioTranslation("editBackupTask");

    const isEditing = taskId != null;
    const [cronServerErrors] = useState<CronServerErrors>(() => new Map());

    const form = useForm<EditBackupTaskFormData>({
        mode: "all",
        resolver: editBackupTaskYupResolver,
        context: {
            isManual,
            cronServerErrors,
            isDatabaseEncrypted: db.isEncrypted,
        } satisfies EditBackupTaskValidationContext,
        defaultValues: editBackupTaskUtils.getDefaultValues({
            dto,
            isDatabaseEncrypted: db.isEncrypted,
            isSharded: db.isSharded,
            localRootPath: serverLimits.LocalRootPath,
        }),
    });

    const { setIsDirty } = useDirtyFlag(form.formState.isDirty);

    const steps = getEditBackupTaskSteps(isManual, t);
    const [currentStepId, setCurrentStepId] = useState<EditBackupTaskStepId>(isEditing ? "summary" : "type");
    const { closeThen } = useViewSheet();

    const goToStep = (stepId: EditBackupTaskStepId) => closeThen(() => setCurrentStepId(stepId));

    const close = () => {
        if (sourceView === "OngoingTasks") {
            router.navigate(appUrl.forOngoingTasks(db.name));
        } else {
            router.navigate(appUrl.forBackups(db.name));
        }
    };

    const handleSave: SubmitHandler<EditBackupTaskFormData> = (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("Backup", isManual ? "manual" : "periodic");

            if (isManual) {
                const result = await tasksService.startManualBackup(
                    db.name,
                    editBackupTaskUtils.mapToManualBackupDto(formData, serverLimits.LocalRootPath)
                );
                notificationCenter.instance.openDetailsForOperationById(
                    activeDatabaseTracker.default.database(),
                    result.OperationId
                );
            } else {
                await tasksService.savePeriodicBackupConfiguration(
                    db.name,
                    editBackupTaskUtils.mapToPeriodicBackupDto(
                        formData,
                        taskId,
                        serverLimits.LocalRootPath,
                        dto?.CreatedAt ?? null
                    )
                );
            }

            setIsDirty(false);
            close();
        });
    };

    const handleInvalid: SubmitErrorHandler<EditBackupTaskFormData> = (errors) => {
        const stepWithError = steps.find((step) => getEditBackupTaskStepFields(step.id).some((field) => errors[field]));
        if (stepWithError) {
            goToStep(stepWithError.id);
        }
    };

    const context: EditBackupTaskContext = {
        isManual,
        isEditing,
        taskId,
        serverLimits,
        cronServerErrors,
        goToStep,
    };

    return (
        <FormProvider {...form}>
            <WizardLayout
                className="edit-backup-task"
                onSubmit={form.handleSubmit(handleSave, handleInvalid)}
                content={
                    <>
                        {!hasDatabaseAdminAccess && (
                            <RichAlert variant="info" className="edit-backup-task-alert mb-4">
                                {t("accessRequired", { context: isEditing ? "edit" : "new" })}
                            </RichAlert>
                        )}
                        <EditBackupTaskHeader stepId={currentStepId} isManual={isManual} />
                        <CurrentStep stepId={currentStepId} context={context} />
                    </>
                }
                footer={
                    <EditBackupTaskFooter
                        steps={steps}
                        currentStepId={currentStepId}
                        context={context}
                        onCancel={close}
                    />
                }
                sidebar={
                    <WizardStepsList<EditBackupTaskStepId>
                        steps={steps}
                        currentStepId={currentStepId}
                        onStepClick={goToStep}
                    />
                }
            />
        </FormProvider>
    );
}

function CurrentStep({ stepId, context }: { stepId: EditBackupTaskStepId; context: EditBackupTaskContext }) {
    switch (stepId) {
        case "type":
            return <EditBackupTaskStepType context={context} />;
        case "settings":
            return <EditBackupTaskStepSettings context={context} />;
        case "schedule":
            return <EditBackupTaskStepSchedule context={context} />;
        case "destinations":
            return <EditBackupTaskStepDestinations context={context} />;
        case "summary":
            return <EditBackupTaskStepSummary context={context} />;
        default:
            return assertUnreachable(stepId);
    }
}

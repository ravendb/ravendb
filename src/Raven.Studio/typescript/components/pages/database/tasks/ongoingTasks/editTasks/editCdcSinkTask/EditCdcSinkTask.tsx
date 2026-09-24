import "./EditCdcSinkTask.scss";
import { AboutViewHeading } from "components/common/AboutView";
import { useServices } from "components/hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { FormProvider, SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { LoadingView } from "components/common/LoadingView";
import { useAsyncCallback } from "react-async-hook";
import { LoadError } from "components/common/LoadError";
import { EditCdcSinkTaskFormData, editCdcSinkTaskResolver } from "./utils/editCdcSinkTaskValidation";
import EditCdcSinkTaskBasicSection from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/basic/EditCdcSinkTaskBasicSection";
import EditCdcSinkTaskDiscoverySection from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/discovery/EditCdcSinkTaskDiscoverySection";
import EditCdcSinkTaskTablesSection from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/EditCdcSinkTaskTablesSection";
import { editCdcSinkTaskUtils } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskUtils";
import { tryHandleSubmit } from "components/utils/common";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useAppUrls } from "components/hooks/useAppUrls";
import router from "plugins/router";
import { SubmitEventHandler, useEffect } from "react";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import EditCdcSinkTaskRawView from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskRawView";
import SizeGetter from "components/common/SizeGetter";
import EditCdcSinkTaskRawViewSwitch from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskRawViewSwitch";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import EditCdcSinkTaskInfoHub from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskInfoHub";
import classNames from "classnames";
import EditCdcSinkTaskFooter from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskFooter";
import { useEditCdcSinkTaskVerification } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskVerification";
import EditCdcSinkTaskVerificationAlert from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskVerificationAlert";
import useConfirm from "components/common/ConfirmDialog";
import { useEditCdcSinkTaskRawViewSync } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskRawViewSync";

interface QueryParams {
    taskId?: string;
}

export default function EditCdcSinkTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isRawView = useAppSelector(editCdcSinkTaskSelectors.isRawView);
    const isRawViewDirty = useAppSelector(editCdcSinkTaskSelectors.isRawViewDirty);
    const hasCdcSink = useAppSelector(licenseSelectors.statusValue("HasCdcSink"));

    const taskId = queryParams?.taskId ? parseInt(queryParams.taskId, 10) : null;
    const isEditMode = taskId != null;

    // Reset store on unmount
    useEffect(() => {
        return () => {
            dispatch(editCdcSinkTaskActions.reset());
        };
    }, []);

    const asyncGetDefaultValues = useAsyncCallback(async () => {
        if (isEditMode) {
            dispatch(editCdcSinkTaskActions.taskIdSet(taskId));
            const taskInfo = await tasksService.getCdcSinkTaskInfo(databaseName, taskId);
            return editCdcSinkTaskUtils.mapTaskFromDto(taskInfo);
        } else {
            return editCdcSinkTaskUtils.mapTaskFromDto(null);
        }
    });

    const editForm = useForm<EditCdcSinkTaskFormData>({
        mode: "onTouched",
        defaultValues: asyncGetDefaultValues.execute,
        resolver: editCdcSinkTaskResolver,
    });

    const isDirty = editForm.formState.isDirty || isRawViewDirty;
    const { setIsDirty } = useDirtyFlag(isDirty);
    const { appUrl } = useAppUrls();
    const confirm = useConfirm();
    const asyncVerify = useEditCdcSinkTaskVerification(editForm);
    const { applyRawViewContent, revealValidationErrors } = useEditCdcSinkTaskRawViewSync(editForm);

    const tablesFieldArray = useFieldArray({
        control: editForm.control,
        name: "tables",
    });

    const reloadEditForm = async () => {
        const result = await asyncGetDefaultValues.execute();
        editForm.reset(result);
    };

    const cancel = () => {
        router.navigate(appUrl.forOngoingTasks(databaseName));
    };

    const handleSubmit: SubmitHandler<EditCdcSinkTaskFormData> = (formData) => {
        return tryHandleSubmit(async () => {
            const result = asyncVerify.getCurrentResult(formData) ?? (await asyncVerify.verify(formData));

            if (!result) {
                return;
            }

            if (!result.Success || result.Warnings.length > 0) {
                const isConfirmed = await confirm({
                    title: "Save the task configuration anyway?",
                    message: <EditCdcSinkTaskVerificationAlert result={result} />,
                    icon: result.Success ? "warning" : "danger",
                    actionColor: result.Success ? "warning" : "danger",
                    confirmText: "Save anyway",
                    confirmIcon: "save",
                    size: "lg",
                });

                if (!isConfirmed) {
                    return;
                }
            }

            await tasksService.saveCdcSinkTask(databaseName, editCdcSinkTaskUtils.mapToDto(formData, taskId));
            editForm.reset(formData);
            dispatch(editCdcSinkTaskActions.rawViewContentSaved());
            setIsDirty(false);
            cancel();
        });
    };

    const onSubmit: SubmitEventHandler<HTMLFormElement> = (e) => {
        if (!applyRawViewContent()) {
            e.preventDefault();
            return;
        }

        return editForm.handleSubmit(handleSubmit, revealValidationErrors)(e);
    };

    if (asyncGetDefaultValues.loading) {
        return <LoadingView />;
    }

    if (asyncGetDefaultValues.error) {
        return <LoadError error="Unable to load configuration" refresh={reloadEditForm} />;
    }

    return (
        <FormProvider {...editForm}>
            <form onSubmit={onSubmit} className="edit-cdc-sink-task vstack h-100 w-100">
                <div className="p-3 flex-grow-1 overflow-y-auto vstack">
                    <div className="hstack align-items-center flex-wrap gap-2">
                        <AboutViewHeading
                            title={isEditMode ? "Edit CDC Sink task" : "New CDC Sink task"}
                            licenseBadgeText={hasCdcSink ? null : "Enterprise"}
                            icon="sql-etl"
                            marginBottom={4}
                            className="me-auto"
                        />
                        <EditCdcSinkTaskRawViewSwitch taskId={taskId} isDisabled={!hasCdcSink} />
                        <EditCdcSinkTaskInfoHub />
                    </div>
                    {isRawView ? (
                        <div className="flex-grow-1">
                            <SizeGetter
                                isHeighRequired
                                render={({ height }) => <EditCdcSinkTaskRawView heightPx={height} />}
                            />
                        </div>
                    ) : (
                        <div className={classNames("vstack h-100", { "form-disabled": !hasCdcSink })}>
                            <EditCdcSinkTaskBasicSection />
                            <EditCdcSinkTaskDiscoverySection tablesFieldArray={tablesFieldArray} />
                            <EditCdcSinkTaskTablesSection tablesFieldArray={tablesFieldArray} />
                        </div>
                    )}
                </div>
                <EditCdcSinkTaskFooter
                    asyncVerify={asyncVerify}
                    isDirty={isDirty}
                    isDisabled={!hasCdcSink}
                    onCancel={cancel}
                />
            </form>
        </FormProvider>
    );
}

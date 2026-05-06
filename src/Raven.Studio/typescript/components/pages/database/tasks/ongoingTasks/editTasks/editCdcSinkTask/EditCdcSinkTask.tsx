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
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useEffect } from "react";
import { editCdcSinkTaskActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";

interface QueryParams {
    taskId?: string;
}

export default function EditCdcSinkTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

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
            const taskInfo = await tasksService.getCdcSinkTaskInfo(databaseName, taskId);
            return editCdcSinkTaskUtils.mapFromDto(taskInfo);
        } else {
            return editCdcSinkTaskUtils.mapFromDto(null);
        }
    });

    const editForm = useForm<EditCdcSinkTaskFormData>({
        defaultValues: asyncGetDefaultValues.execute,
        resolver: editCdcSinkTaskResolver,
    });

    const { setIsDirty } = useDirtyFlag(editForm.formState.isDirty);
    const { appUrl } = useAppUrls();

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
            await tasksService.saveCdcSinkTask(databaseName, editCdcSinkTaskUtils.mapToDto(formData, taskId));
            editForm.reset(formData);
            setIsDirty(false);
            cancel();
        });
    };

    console.log("kalczur errors", editForm.formState.errors);

    if (asyncGetDefaultValues.loading) {
        return <LoadingView />;
    }

    if (asyncGetDefaultValues.error) {
        return <LoadError error="Unable to load configuration" refresh={reloadEditForm} />;
    }

    return (
        <FormProvider {...editForm}>
            <form onSubmit={editForm.handleSubmit(handleSubmit)} className="edit-cdc-sink-task vstack h-100 w-100">
                <div className="p-3 flex-grow-1 overflow-y-auto">
                    <AboutViewHeading
                        title={isEditMode ? "Edit CDC Sink task" : "New CDC Sink task"}
                        icon="sql-etl"
                        marginBottom={4}
                    />
                    <EditCdcSinkTaskBasicSection />
                    <EditCdcSinkTaskDiscoverySection tablesFieldArray={tablesFieldArray} />
                    <EditCdcSinkTaskTablesSection tablesFieldArray={tablesFieldArray} />
                </div>
                <div className="hstack justify-content-between gap-2 py-2 px-3 border-top border-secondary">
                    <Button variant="outline-secondary" className="rounded-pill" onClick={cancel}>
                        Cancel
                    </Button>
                    <Button
                        type="submit"
                        variant="primary"
                        className="rounded-pill"
                        disabled={!editForm.formState.isDirty}
                    >
                        <Icon icon="save" />
                        Save task configuration
                    </Button>
                </div>
            </form>
        </FormProvider>
    );
}

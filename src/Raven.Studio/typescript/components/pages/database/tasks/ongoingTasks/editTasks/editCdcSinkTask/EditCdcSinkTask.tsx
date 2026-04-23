import { AboutViewHeading } from "components/common/AboutView";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { FormProvider, useFieldArray, useForm } from "react-hook-form";
import { LoadingView } from "components/common/LoadingView";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { LoadError } from "components/common/LoadError";
import DatabaseUtils from "components/utils/DatabaseUtils";
import {
    EditCdcSinkTaskFormData,
    EditCdcSinkTaskValidationContext,
    editCdcSinkTaskResolver,
} from "./utils/editCdcSinkTaskValidation";
import EditCdcSinkTaskBasicSection from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskBasicSection";
import EditCdcSinkTaskExplorer from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskExplorer";
import EditCdcSinkTaskTables from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskTables";

interface QueryParams {
    taskId?: string;
}

export default function EditCdcSinkTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const { tasksService } = useServices();
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);
    const taskId = queryParams?.taskId ? parseInt(queryParams.taskId, 10) : null;
    const isEditMode = taskId != null;

    const asyncGetDefaultValues = useAsyncCallback(async () => {
        if (isEditMode) {
            const taskInfo = await tasksService.getCdcSinkTaskInfo(databaseName, taskId);
            return getDefaultValues(taskInfo);
        } else {
            return getDefaultValues(null);
        }
    });

    const asyncGetTaskNames = useAsync(async () => {
        if (!db) {
            return [];
        }

        const location = DatabaseUtils.getFirstLocation(db, localNodeTag);
        const ongoingTasks = await tasksService.getOngoingTasks(databaseName, location);

        return ongoingTasks.OngoingTasks.map((x) => x.TaskName);
    }, [db, localNodeTag, databaseName]);

    const validationContext: EditCdcSinkTaskValidationContext = {
        initialTaskName: asyncGetDefaultValues.result?.name,
        usedTaskNames: asyncGetTaskNames.result ?? [],
    };

    const form = useForm<EditCdcSinkTaskFormData, EditCdcSinkTaskValidationContext>({
        mode: "all",
        resolver: editCdcSinkTaskResolver,
        context: validationContext,
        defaultValues: asyncGetDefaultValues.execute,
    });

    const tablesFieldArray = useFieldArray({
        control: form.control,
        name: "tables",
    });

    const reloadEditForm = async () => {
        const result = await asyncGetDefaultValues.execute();
        form.reset(result);
    };

    if (asyncGetDefaultValues.loading || asyncGetTaskNames.loading) {
        return <LoadingView />;
    }

    if (asyncGetDefaultValues.error || asyncGetTaskNames.error) {
        return <LoadError error="Unable to load configuration" refresh={reloadEditForm} />;
    }

    return (
        <div className="content-margin">
            <AboutViewHeading
                title={isEditMode ? "Edit CDC Sink task" : "New CDC Sink task"}
                icon="sql-etl"
                marginBottom={4}
            />
            <FormProvider {...form}>
                <form onSubmit={form.handleSubmit(console.log)}>
                    <EditCdcSinkTaskBasicSection />
                    <EditCdcSinkTaskExplorer tablesFieldArray={tablesFieldArray} />
                    <EditCdcSinkTaskTables tablesFieldArray={tablesFieldArray} />
                </form>
            </FormProvider>
        </div>
    );
}

function getDefaultValues(
    task: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink
): EditCdcSinkTaskFormData {
    if (!task) {
        return {
            name: "",
            state: "Enabled",
            isSetResponsibleNode: false,
            responsibleNode: "",
            isPinResponsibleNode: false,
            connectionStringName: "",
            skipInitialLoad: false,
            postgresPublicationName: "",
            postgresSlotName: "",
            tables: [],
        };
    }

    const configuration = task.Configuration;

    return {
        name: configuration.Name,
        state: configuration.Disabled ? "Disabled" : "Enabled",
        isSetResponsibleNode: Boolean(configuration.MentorNode),
        responsibleNode: configuration.MentorNode ?? "",
        isPinResponsibleNode: configuration.PinToMentorNode ?? false,
        connectionStringName: configuration.ConnectionStringName ?? "",
        skipInitialLoad: configuration.SkipInitialLoad ?? false,
        postgresPublicationName: configuration.Postgres?.PublicationName ?? "",
        postgresSlotName: configuration.Postgres?.SlotName ?? "",
        tables: configuration.Tables ?? [],
    };
}

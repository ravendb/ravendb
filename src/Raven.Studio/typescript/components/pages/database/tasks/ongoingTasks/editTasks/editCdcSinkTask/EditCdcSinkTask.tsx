import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { tryHandleSubmit } from "components/utils/common";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { editCdcSinkTaskUtils } from "./utils";
import { editCdcSinkTaskResolver } from "./validation";
import { CdcSinkFormData } from "./types";
import CdcSinkService from "./services/cdcSinkService";
import CdcSinkBasicFields from "./partials/CdcSinkBasicFields";
import CdcSinkSchemaExplorer from "./partials/CdcSinkSchemaExplorer";
import CdcSinkTableList from "./partials/CdcSinkTableList";
import CdcSinkTableEditor from "./partials/CdcSinkTableEditor";
import getOngoingTaskInfoCommand from "commands/database/tasks/getOngoingTaskInfoCommand";
import { useState } from "react";
import router from "plugins/router";

interface QueryParams {
    taskId: string;
}

export default function EditCdcSinkTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const { tasksService } = useServices();
    const { reportEvent } = useEventsCollector();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { appUrl } = useAppUrls();

    const taskId = queryParams?.taskId ? parseInt(queryParams.taskId) : null;
    const isEditMode = taskId != null;

    const [editingTableIndex, setEditingTableIndex] = useState<number | null>(null);

    const getDefaultValues = async (): Promise<CdcSinkFormData> => {
        if (taskId) {
            const result = await getOngoingTaskInfoCommand.forCdcSink(databaseName, taskId).execute();
            return editCdcSinkTaskUtils.getDefaultValues(result.Configuration);
        }
        return editCdcSinkTaskUtils.getDefaultValues();
    };

    const form = useForm<CdcSinkFormData>({
        mode: "all",
        resolver: editCdcSinkTaskResolver as any,
        defaultValues: getDefaultValues,
    });

    const { setIsDirty } = useDirtyFlag(form.formState.isDirty);
    const { handleSubmit, reset, formState } = form;

    const navigateToOngoingTasks = () => {
        router.navigate(appUrl.forOngoingTasks(databaseName));
    };

    const handleSave: SubmitHandler<CdcSinkFormData> = (data) => {
        return tryHandleSubmit(async () => {
            reportEvent("cdc-sink", "save");

            const dto = editCdcSinkTaskUtils.mapToDto(data, taskId);
            await CdcSinkService.save(databaseName, dto);

            reset(data);
            setIsDirty(false);
            navigateToOngoingTasks();
        });
    };

    return (
        <FormProvider {...form}>
            <form onSubmit={handleSubmit(handleSave)}>
                <div className="content-margin">
                    <div className="d-flex justify-content-between align-items-center mb-4">
                        <h2>
                            <Icon icon="sql-etl" />
                            {isEditMode ? "Edit CDC Sink Task" : "New CDC Sink Task"}
                        </h2>
                        <div className="d-flex gap-2">
                            <button type="button" className="btn btn-secondary" onClick={navigateToOngoingTasks}>
                                Cancel
                            </button>
                            <ButtonWithSpinner
                                type="submit"
                                variant="primary"
                                icon="save"
                                isSpinning={formState.isSubmitting}
                                disabled={!formState.isDirty || formState.isSubmitting}
                            >
                                Save
                            </ButtonWithSpinner>
                        </div>
                    </div>

                    <CdcSinkBasicFields />

                    <hr />

                    <CdcSinkSchemaExplorer />

                    <hr />

                    {editingTableIndex != null ? (
                        <CdcSinkTableEditor
                            tableIndex={editingTableIndex}
                            onClose={() => setEditingTableIndex(null)}
                        />
                    ) : (
                        <CdcSinkTableList
                            onEditTable={(index) => setEditingTableIndex(index)}
                        />
                    )}
                </div>
            </form>
        </FormProvider>
    );
}

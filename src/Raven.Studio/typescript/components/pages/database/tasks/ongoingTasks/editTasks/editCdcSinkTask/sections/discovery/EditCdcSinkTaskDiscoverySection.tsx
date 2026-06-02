import CollapseButton from "components/common/CollapseButton";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import Collapse from "react-bootstrap/Collapse";
import { UseFieldArrayReturn, useFormContext, useWatch } from "react-hook-form";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import EditCdcSinkTaskDiscoveredTable from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/discovery/EditCdcSinkTaskDiscoveredTable";
import SizeGetter from "components/common/SizeGetter";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import EditCdcSinkTaskDiscoverySchemasModal from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/discovery/EditCdcSinkTaskDiscoverySchemasModal";
import { useAppDispatch } from "components/store";
import { editCdcSinkTaskActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { useEffect } from "react";

interface EditCdcSinkTaskDiscoverySectionProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskDiscoverySection({ tablesFieldArray }: EditCdcSinkTaskDiscoverySectionProps) {
    const { tasksService } = useServices();
    const dispatch = useAppDispatch();
    const { value: isPanelOpen, toggle: toggleIsPanelOpen, setTrue: openPanel } = useBoolean(true);
    const { value: isSchemasModalOpen, setTrue: openSchemasModal, setFalse: closeSchemasModal } = useBoolean(false);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { control, getValues } = useFormContext<EditCdcSinkTaskFormData>();
    const connectionStringName = useWatch({
        control,
        name: "connectionStringName",
    });

    const asyncGetSchema = useAsyncCallback(async (schemas: string[]) => {
        openPanel();
        dispatch(editCdcSinkTaskActions.sourceSchemaSet(null));

        const sourceSchema = await tasksService.getCdcSinkTaskSchema(databaseName, {
            Schemas: schemas,
            Connection: null,
            ConnectionStringName: connectionStringName,
        });

        dispatch(editCdcSinkTaskActions.sourceSchemaSet(sourceSchema));

        return sourceSchema;
    });

    // When connection string changes, fetch the schema
    useEffect(() => {
        const tables = getValues("tables");

        // Get all unique, non-empty schemas from the tables. If there are none, pass null to get the default schema.
        const allUsedSchemas = Array.from(
            new Set((tables ?? []).map((table) => table.sourceTableSchema?.trim()).filter(Boolean))
        );

        if (connectionStringName) {
            asyncGetSchema.execute(allUsedSchemas.length > 0 ? allUsedSchemas : null);
        }
    }, [connectionStringName]);

    return (
        <div className="mt-3">
            <div className="hstack align-items-end gap-2">
                <div>
                    <div className="hstack align-items-center">
                        <h3 className="m-0">Schema Explorer</h3>
                        <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
                    </div>
                    <div className="mb-1">
                        Discover existing tables from the configured connection.
                        <br />
                        After tables are shown, select the ones you want and click &quot;Configure selected tables&quot;
                        to add them below.
                    </div>
                </div>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !connectionStringName,
                            message: "A connection string is required to discover tables.",
                        },
                        {
                            isActive: !!connectionStringName,
                            message: "Click to fetch tables from the source database.",
                        },
                    ]}
                    className="ms-auto"
                >
                    <ButtonWithSpinner
                        variant="secondary"
                        className="rounded-pill"
                        onClick={openSchemasModal}
                        disabled={!connectionStringName}
                        isSpinning={asyncGetSchema.loading}
                        icon="search"
                    >
                        Discover tables
                    </ButtonWithSpinner>
                </ConditionalPopover>
                {isSchemasModalOpen && (
                    <EditCdcSinkTaskDiscoverySchemasModal onClose={closeSchemasModal} asyncGetSchema={asyncGetSchema} />
                )}
            </div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="vstack gap-2 mt-2">
                        <SizeGetter
                            render={({ width }) => (
                                <EditCdcSinkTaskDiscoveredTable
                                    asyncGetSchema={asyncGetSchema}
                                    tablesFieldArray={tablesFieldArray}
                                    widthPx={width}
                                />
                            )}
                        />
                    </div>
                </div>
            </Collapse>
        </div>
    );
}

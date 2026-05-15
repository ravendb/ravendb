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
import EditCdcSinkTaskVerifyResult from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/discovery/EditCdcSinkTaskVerifyResult";
import EditCdcSinkTaskDiscoverySchemasModal from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/discovery/EditCdcSinkTaskDiscoverySchemasModal";

interface EditCdcSinkTaskDiscoverySectionProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskDiscoverySection({ tablesFieldArray }: EditCdcSinkTaskDiscoverySectionProps) {
    const { tasksService } = useServices();
    const { value: isPanelOpen, toggle: toggleIsPanelOpen, setTrue: openPanel } = useBoolean(true);
    const { value: isSchemasModalOpen, setTrue: openSchemasModal, setFalse: closeSchemasModal } = useBoolean(false);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const connectionStringName = useWatch({
        control,
        name: "connectionStringName",
    });

    const asyncGetSchema = useAsyncCallback(async (schemas: string[]) => {
        openPanel();

        return await tasksService.getCdcSinkTaskSchema(databaseName, {
            Schemas: schemas,
            Connection: null,
            ConnectionStringName: connectionStringName,
        });
    });

    const asyncVerifySource = useAsyncCallback(async () => {
        openPanel();

        return await tasksService.verifyCdcSink(databaseName, {
            ConnectionStringName: connectionStringName,
            TableNames: asyncGetSchema.result?.Tables?.map((t) => t.SourceTableName) ?? [],
        });
    });

    const hasTables = Boolean(asyncGetSchema.result?.Tables?.length);

    return (
        <div className="mt-3">
            <div className="hstack align-items-end gap-2">
                <div>
                    <div className="hstack align-items-center">
                        <h3 className="m-0">Schema Explorer</h3>
                        <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
                    </div>
                    <div className="mb-1">Fetch existing tables from the linked source.</div>
                </div>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !connectionStringName,
                            message: "Please provide a connection string to fetch tables.",
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
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !connectionStringName,
                            message: "Please provide a connection string to verify source.",
                        },
                        {
                            isActive: !hasTables,
                            message: "Please discover tables to verify the source connection.",
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="secondary"
                        className="rounded-pill"
                        onClick={asyncVerifySource.execute}
                        isSpinning={asyncVerifySource.loading}
                        disabled={!hasTables || !connectionStringName}
                        icon="test"
                    >
                        Verify source
                    </ButtonWithSpinner>
                </ConditionalPopover>
            </div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="vstack gap-2 mt-2">
                        <EditCdcSinkTaskVerifyResult asyncVerifySource={asyncVerifySource} />
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

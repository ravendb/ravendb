import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import CollapseButton from "components/common/CollapseButton";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { sortBy } from "lodash";
import { useAsync } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import InputGroup from "react-bootstrap/InputGroup";
import Collapse from "react-bootstrap/Collapse";
import { FormErrorIcon, FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { useEffect, useMemo } from "react";
import { editCdcSinkTaskActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { FormTaskResponsibleNode } from "components/common/formFields/FormTaskResponsibleNode";

type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

export default function EditCdcSinkTaskBasicSection() {
    const dispatch = useAppDispatch();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { value: isNewConnectionStringOpen, toggle: toggleIsNewConnectionStringOpen } = useBoolean(false);
    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    const { control, setValue } = useFormContext<EditCdcSinkTaskFormData>();
    const formValues = useWatch({ control });

    const asyncGetConnectionStrings = useAsync(async () => {
        if (!databaseName) {
            return {};
        }

        const result = await tasksService.getConnectionStrings(databaseName);
        return result.SqlConnectionStrings ?? {};
    }, [databaseName]);

    const sqlConnectionStrings = useMemo(
        () => asyncGetConnectionStrings.result ?? {},
        [asyncGetConnectionStrings.result]
    );

    const connectionStringOptions: SelectOption[] = sortBy(Object.values(sqlConnectionStrings), (x) =>
        x.Name.toUpperCase()
    ).map((x) => ({
        value: x.Name,
        label: x.Name,
    }));

    const selectedConnectionString = useMemo(
        () => (formValues.connectionStringName ? sqlConnectionStrings[formValues.connectionStringName] : null),
        [formValues.connectionStringName, sqlConnectionStrings]
    );

    // Sync selected connection string to the store
    useEffect(() => {
        dispatch(editCdcSinkTaskActions.connectionStringSelected(selectedConnectionString));
    }, [selectedConnectionString]);

    const hasPostgresSettings =
        selectedConnectionString?.FactoryName === "Npgsql" ||
        Boolean(formValues.postgresPublicationName || formValues.postgresSlotName);

    const handleConnectionStringSave = async (connectionName: string) => {
        await asyncGetConnectionStrings.execute();

        setValue("connectionStringName", connectionName, {
            shouldValidate: true,
            shouldTouch: true,
            shouldDirty: true,
        });

        toggleIsNewConnectionStringOpen();
    };

    return (
        <div>
            <div className="hstack align-items-center">
                <h3 className="m-0">Configure basic settings</h3>
                <FormErrorIcon
                    control={control}
                    paths={["name", "connectionStringName", "responsibleNode"]}
                    onError={() => setIsPanelOpen(true)}
                />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">
                Specify the task name, enter a connection string, and verify the source connection.
            </div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <FormGroup>
                            <FormLabel>Task Name</FormLabel>
                            <FormInput type="text" control={control} name="name" placeholder="My CDC Sink task" />
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>Task State</FormLabel>
                            <FormSelect control={control} name="state" options={taskStateOptions} />
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>Connection String</FormLabel>
                            <InputGroup>
                                <FormSelect
                                    control={control}
                                    name="connectionStringName"
                                    options={connectionStringOptions}
                                    isLoading={asyncGetConnectionStrings.loading}
                                />
                                <InputGroup.Text>
                                    <ButtonWithSpinner
                                        variant="link"
                                        className="text-reset px-0"
                                        icon="plus"
                                        isSpinning={asyncGetConnectionStrings.loading}
                                        onClick={toggleIsNewConnectionStringOpen}
                                    >
                                        Create a new SQL connection string
                                    </ButtonWithSpinner>
                                </InputGroup.Text>
                            </InputGroup>
                            {isNewConnectionStringOpen && (
                                <EditConnectionStrings
                                    initialConnection={{ type: "Sql" }}
                                    afterSave={handleConnectionStringSave}
                                    afterClose={toggleIsNewConnectionStringOpen}
                                />
                            )}
                        </FormGroup>
                        {hasPostgresSettings && (
                            <>
                                <RichAlert variant="info">
                                    PostgreSQL connections can use a custom publication name and replication slot. Leave
                                    them empty to let the server auto-generate both values.
                                </RichAlert>
                                <FormGroup className="mt-2">
                                    <FormLabel>Publication Name</FormLabel>
                                    <FormInput
                                        type="text"
                                        control={control}
                                        name="postgresPublicationName"
                                        placeholder="rvn_cdc_p_..."
                                    />
                                </FormGroup>
                                <FormGroup>
                                    <FormLabel>Slot Name</FormLabel>
                                    <FormInput
                                        type="text"
                                        control={control}
                                        name="postgresSlotName"
                                        placeholder="rvn_cdc_s_..."
                                    />
                                </FormGroup>
                            </>
                        )}
                        <FormTaskResponsibleNode
                            control={control}
                            isSetName="isSetResponsibleNode"
                            nodeName="responsibleNode"
                            isPinName="isPinResponsibleNode"
                        />
                        <FormGroup>
                            <FormSwitch control={control} name="skipInitialLoad">
                                Skip initial load
                            </FormSwitch>
                        </FormGroup>
                    </div>
                </div>
            </Collapse>
        </div>
    );
}

const taskStateOptions: SelectOption<OngoingTaskState>[] = [
    { value: "Enabled", label: "Enabled" },
    { value: "Disabled", label: "Disabled" },
];

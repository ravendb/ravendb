import CollapseButton from "components/common/CollapseButton";
import { SelectOption } from "components/common/select/Select";
import useBoolean from "components/hooks/useBoolean";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import { FormErrorIcon, FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { FormTaskResponsibleNode } from "components/common/formFields/FormTaskResponsibleNode";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { FormTaskConnectionString } from "components/common/formFields/FormTaskConnectionString";
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";

type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

export default function EditCdcSinkTaskBasicSection() {
    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const [connectionStringName, postgresPublicationName, postgresSlotName] = useWatch({
        control,
        name: ["connectionStringName", "postgresPublicationName", "postgresSlotName"],
    });
    const sqlConnections = useAppSelector(connectionStringSelectors.connectionsByType("Sql"));
    const selectedConnection = sqlConnections.find((x) => x.name === connectionStringName);

    const hasPostgresSettings =
        (selectedConnection && "factoryName" in selectedConnection && selectedConnection.factoryName === "Npgsql") ||
        Boolean(postgresPublicationName || postgresSlotName);

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
            <div className="mb-1">Specify the task name, select a connection string, and configure task options.</div>
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
                        <FormTaskConnectionString control={control} name="connectionStringName" type="Sql" />
                        {hasPostgresSettings && (
                            <>
                                <RichAlert variant="info">
                                    For PostgreSQL connections, you can specify a custom publication name and
                                    replication slot name. Leave these fields empty to let the server auto-generate both
                                    values.
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
                                <PopoverWithHoverWrapper
                                    message={
                                        <>
                                            By default, the task takes a snapshot of existing rows before streaming new
                                            changes.
                                            <br />
                                            Enable this option to stream only new changes.
                                        </>
                                    }
                                >
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
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

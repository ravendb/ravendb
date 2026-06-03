import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormSwitch, FormGroup, FormLabel, FormSelect } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import { useAppDispatch, useAppSelector } from "components/store";
import { sortBy } from "lodash";
import { useAsync } from "react-async-hook";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import InputGroup from "react-bootstrap/InputGroup";
import { useServices } from "components/hooks/useServices";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData, GenAiStartingPoint } from "../../utils/editGenAiTaskValidation";
import TaskUtils from "components/utils/TaskUtils";
import OptionalLabel from "components/common/OptionalLabel";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { editGenAiTaskUtils } from "../../utils/editGenAiTaskUtils";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import tasksCommonContent from "models/database/tasks/tasksCommonContent";
import { FormTaskResponsibleNode } from "components/common/formFields/FormTaskResponsibleNode";

type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

export default function EditGenAiTaskBasicFields() {
    const dispatch = useAppDispatch();

    const isEncrypted = useAppSelector(databaseSelectors.activeDatabase)?.isEncrypted;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isEditTask = useAppSelector(editGenAiTaskSelectors.isEditTask);

    const { value: isNewConnectionStringOpen, toggle: toggleIsNewConnectionStringOpen } = useBoolean(false);

    const { tasksService } = useServices();

    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    const asyncGetConnectionStringsOptions = useAsync(async () => {
        const result = await tasksService.getConnectionStrings(databaseName);

        dispatch(editGenAiTaskActions.aiConnectionStringsSet(result.AiConnectionStrings));

        const connectionStrings = Object.values(result.AiConnectionStrings)
            .filter((x) => x.ModelType === "Chat")
            .map((x) => x.Name);

        return sortBy(connectionStrings, (x) => x.toUpperCase()).map(
            (x) => ({ value: x, label: x }) satisfies SelectOption
        );
    }, []);

    const handleConnectionStringSave = async (connectionName: string) => {
        await asyncGetConnectionStringsOptions.execute();
        setValue("connectionStringName", connectionName, {
            shouldValidate: true,
            shouldTouch: true,
            shouldDirty: true,
        });
        toggleIsNewConnectionStringOpen();
    };

    const handleGenerateIdentifier = () => {
        setValue("identifier", TaskUtils.getGeneratedIdentifier(formValues.name));
    };

    return (
        <>
            <FormGroup>
                <FormLabel>Task Name</FormLabel>
                <ConditionalPopover
                    conditions={{
                        isActive: isEditTask,
                        message: tasksCommonContent.taskNameLocked,
                    }}
                    className="w-100"
                >
                    <FormInput
                        type="text"
                        control={control}
                        name="name"
                        placeholder="My task"
                        onBlur={() => {
                            if (!formValues.identifier) {
                                handleGenerateIdentifier();
                            }
                        }}
                        disabled={isEditTask}
                    />
                </ConditionalPopover>
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    Identifier <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                A unique identifier for the task.
                                <br />
                                Used in the source document metadata to hold an array of unique hashes representing each
                                model request.
                                <br />
                                <br />
                                If not specified, it will be auto-generated from the task name.
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" id="identifier" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    control={control}
                    name="identifier"
                    type="text"
                    placeholder="my-task"
                    addon={
                        <Button
                            variant="link"
                            className="text-reset px-0"
                            onClick={handleGenerateIdentifier}
                            title="Click to generate the identifier from the task name"
                        >
                            <Icon icon="refresh" />
                            Regenerate
                        </Button>
                    }
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Task State</FormLabel>
                <FormSelect control={control} name="state" options={stateOptions} />
            </FormGroup>
            {isEncrypted && (
                <div className="vstack gap-2">
                    <RichAlert variant="info">
                        Database <strong>{databaseName}</strong> is encrypted
                    </RichAlert>
                    <FormGroup>
                        <FormSwitch control={control} name="isAllowEtlOnNonEncryptedChannel">
                            Allow task on a non-encrypted communication channel
                        </FormSwitch>
                    </FormGroup>
                </div>
            )}
            <FormTaskResponsibleNode
                control={control}
                isSetName="isSetResponsibleNode"
                nodeName="responsibleNode"
                isPinName="isPinResponsibleNode"
            />
            <FormGroup>
                <FormLabel>Connection String</FormLabel>
                <InputGroup>
                    <FormSelect
                        control={control}
                        name="connectionStringName"
                        options={asyncGetConnectionStringsOptions.result ?? []}
                        isLoading={asyncGetConnectionStringsOptions.loading}
                    />
                    <InputGroup.Text>
                        <ButtonWithSpinner
                            variant="link"
                            className="text-reset px-0"
                            icon="plus"
                            isSpinning={asyncGetConnectionStringsOptions.loading}
                            onClick={toggleIsNewConnectionStringOpen}
                        >
                            Create a new AI connection string
                        </ButtonWithSpinner>
                    </InputGroup.Text>
                    {isNewConnectionStringOpen && (
                        <EditConnectionStrings
                            initialConnection={{ type: "Ai", modelType: "Chat" }}
                            afterSave={handleConnectionStringSave}
                            afterClose={toggleIsNewConnectionStringOpen}
                        />
                    )}
                </InputGroup>
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    Max concurrency <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                The maximum number of concurrent requests sent to the model (default:{" "}
                                {editGenAiTaskUtils.defaultMaxConcurrency}).
                                <br />
                                Each request includes: a context object, the prompt, and the JSON schema.
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput type="number" control={control} name="maxConcurrency" />
            </FormGroup>
            <FormGroup>
                <FormSwitch control={control} name="isStartingPoint">
                    Use a defined starting point
                </FormSwitch>
            </FormGroup>
            {formValues.isStartingPoint && (
                <FormGroup>
                    <FormLabel>Send Documents From</FormLabel>
                    <FormSelect control={control} name="startingPointType" options={startingPointTypeOptions} />
                </FormGroup>
            )}
            {formValues.isStartingPoint && formValues.startingPointType === "Change Vector" && (
                <FormGroup>
                    <FormLabel>Change Vector</FormLabel>
                    <FormInput
                        type="textarea"
                        as="textarea"
                        rows={3}
                        control={control}
                        name="startingPointChangeVector"
                        placeholder="Enter change vector to start sending documents from"
                    />
                </FormGroup>
            )}
            {formValues.nextBatchStartingPoint && (
                <FormGroup>
                    <FormLabel>
                        Next Batch Starting Point <small className="text-muted fw-light">(read-only)</small>
                    </FormLabel>
                    <FormInput
                        as="textarea"
                        type="textarea"
                        rows={3}
                        control={control}
                        name="nextBatchStartingPoint"
                        disabled
                    />
                </FormGroup>
            )}
        </>
    );
}

const stateOptions: SelectOption<OngoingTaskState>[] = (["Enabled", "Disabled"] satisfies OngoingTaskState[]).map(
    (x) => ({
        label: x,
        value: x,
    })
);

const startingPointTypeOptions: SelectOption<GenAiStartingPoint>[] = (
    ["Beginning of Time", "Latest Document", "Change Vector"] satisfies GenAiStartingPoint[]
).map((x) => ({
    label: x,
    value: x,
}));

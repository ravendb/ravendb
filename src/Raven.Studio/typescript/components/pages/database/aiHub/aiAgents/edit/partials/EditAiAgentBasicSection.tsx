import AceEditor from "components/common/ace/AceEditor";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormAceEditor, FormGroup, FormInput, FormLabel, FormSelect } from "components/common/Form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import TaskUtils from "components/utils/TaskUtils";
import { sortBy } from "lodash";
import { useRef } from "react";
import ReactAce from "react-ace";
import { useAsync } from "react-async-hook";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";

export default function EditAiAgentBasicSection() {
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const systemPromptRef = useRef<ReactAce>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { tasksService } = useServices();

    const { value: isNewConnectionStringOpen, toggle: toggleIsNewConnectionStringOpen } = useBoolean(false);

    const asyncGetConnectionStringsOptions = useAsync(async () => {
        const result = await tasksService.getConnectionStrings(databaseName);

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
            <h3 className="m-0">Configure basic settings</h3>
            <div className="mb-1">
                Setup basic information about your agent - give it a specific task, database it will connect to and
                format in which agent will respond.
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>Name</FormLabel>
                    <FormInput
                        type="text"
                        control={control}
                        name="name"
                        placeholder="e.g. Customer Service Agent"
                        onBlur={() => {
                            if (!formValues.identifier) {
                                handleGenerateIdentifier();
                            }
                        }}
                    />
                </FormGroup>
                <FormGroup>
                    <FormLabel>
                        Identifier
                        <PopoverWithHoverWrapper message="A unique identifier for the agent">
                            <Icon icon="info" color="info" margin="ms-1" id="identifier" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        control={control}
                        name="identifier"
                        type="text"
                        placeholder="e.g. customer-service-agent"
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
                                initialConnection={{ type: "Ai" }}
                                afterSave={handleConnectionStringSave}
                                afterClose={toggleIsNewConnectionStringOpen}
                            />
                        )}
                    </InputGroup>
                </FormGroup>
                <FormGroup>
                    <FormLabel>Agent description prompt</FormLabel>
                    <FormAceEditor
                        aceRef={systemPromptRef}
                        control={control}
                        name="systemPrompt"
                        mode="text"
                        actions={[{ component: <AceEditor.FullScreenAction /> }]}
                        wrapEnabled
                        setOptions={{
                            indentedSoftWrap: false,
                        }}
                    />
                </FormGroup>
                <SampleObjectAndSchemaFields
                    control={control}
                    setValue={setValue}
                    sampleObjectName="sampleObject"
                    sampleObjectLabel="Sample object"
                    sampleObject={formValues.sampleObject}
                    sampleObjectSyntaxHelp={<div>TODO</div>}
                    jsonSchemaName="outputSchema"
                    jsonSchemaLabel="Output JSON schema"
                    jsonSchema={formValues.outputSchema}
                    jsonSchemaSyntaxHelp={<div>TODO</div>}
                />
            </div>
        </>
    );
}

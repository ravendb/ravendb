import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormGroup, FormInput, FormLabel, FormSelect } from "components/common/Form";
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
import { useAsync } from "react-async-hook";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import Code from "components/common/Code";

interface EditAiAgentBasicSectionProps {
    isEditAiAgent: boolean;
}

export default function EditAiAgentBasicSection({ isEditAiAgent }: EditAiAgentBasicSectionProps) {
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

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
                Define your agent&apos;s purpose, its AI provider connection, and the structure of its responses.
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>Agent name</FormLabel>
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
                        disabled={isEditAiAgent}
                    />
                </FormGroup>
                <FormGroup>
                    <FormLabel>
                        Identifier
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    A unique identifier for the agent.
                                    <br />
                                    If not specified, it will be auto-generated from the agent name.
                                </>
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        control={control}
                        name="identifier"
                        type="text"
                        placeholder="e.g. customer-service-agent"
                        disabled={isEditAiAgent}
                        addon={
                            <Button
                                variant="link"
                                className="text-reset px-0"
                                onClick={handleGenerateIdentifier}
                                title="Click to generate the identifier from the agent name."
                                disabled={isEditAiAgent}
                            >
                                <Icon icon="refresh" />
                                Regenerate
                            </Button>
                        }
                    />
                </FormGroup>
                <FormGroup>
                    <FormLabel>
                        Connection String
                        <PopoverWithHoverWrapper message="The selected connection string determines which LLM the agent will interact with.">
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
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
                        System prompt
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    This prompt defines the agent&apos;s role and capabilities.
                                    <br />
                                    It provides general context to guide the LLM&apos;s responses throughout the
                                    conversation.
                                </>
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        type="textarea"
                        as="textarea"
                        control={control}
                        name="systemPrompt"
                        placeholder={agentDescriptionPlaceholder}
                        rows={7}
                    />
                </FormGroup>
                <SampleObjectAndSchemaFields
                    control={control}
                    setValue={setValue}
                    sampleObjectName="sampleObject"
                    sampleObjectLabel="Sample response object"
                    sampleObject={formValues.sampleObject}
                    sampleObjectSyntaxHelp={<SampleObjectSyntaxHelp />}
                    sampleObjectTooltip={
                        <>
                            Provide a JSON object that defines the structure of the responses you expect to receive from
                            the LLM via the agent in the conversation.
                            <br />
                            <br />
                            This object is not sent to the model directly - RavenDB uses it to generate a JSON schema,
                            which is sent to the model.
                        </>
                    }
                    sampleObjectPlaceholder={sampleObjectPlaceholder}
                    jsonSchemaName="outputSchema"
                    jsonSchemaLabel="Response JSON schema"
                    jsonSchema={formValues.outputSchema}
                    jsonSchemaSyntaxHelp={<JsonSchemaSyntaxHelp />}
                    jsonSchemaTooltip={
                        <>
                            This JSON schema defines the structure of the response you expect the LLM to reply with.
                            <br />
                            It is included in the request sent to the model.
                            <br />
                            <br />
                            If you don&apos;t provide a schema, RavenDB will generate one automatically based on the
                            sample response object.
                            <br />
                            <br />
                            If you provide both a sample object and a schema, the schema takes precedence.
                        </>
                    }
                    helpActionTooltipTitle="Syntax example"
                    canRegenerateSchemaName="canRegenerateSchema"
                />
            </div>
        </>
    );
}

const agentDescriptionPlaceholder = `Describe the agent's purpose and capabilities. 
E.g.: You are a customer support assistant for an e-commerce platform, capable of answering questions about products and orders.
You can also assist with returns, refunds, and order issues by triggering the appropriate action to escalate to the support team.`;

const sampleObjectPlaceholder = `{
    // "ResponseField: "Instruction to the LLM",
    // ...
    // "ResponseField" is a custom field name that you want the LLM to include in its response.
    // The value ("Instruction to the LLM") is a natural-language instruction that tells the LLM what content to return in this field.
} 
Open the (?) icon to view an example.`;

const SampleObjectSyntaxHelp = () => {
    const code = `{
    "Request": "Summarize the customer's request.",
    "Response": "Provide your response.",
    "CustomerId": "Provide the customer ID.",
    "RelatedProducts": ["list of the related products"]
}`;

    return (
        <div>
            <div>Example of a sample object that defines the expected response structure:</div>
            <Code code={code} elementToCopy={code} language="json" />
        </div>
    );
};

const JsonSchemaSyntaxHelp = () => {
    const code = `{
    "name": "MEJIa2ZlclQvQ3VZYlIvTENUQmVHa3JGUXlONzcxZ2dBK2pwYnUybDh0OD0",
    "strict": true,
    "schema": {
        "type": "object",
        "properties": {
            "Request": {
                "type": "string",
                "description": "Summarize the customer's request."
            },
            "Response": {
                "type": "string",
                "description": "Provide your response."
            },
            "CustomerId": {
                "type": "string",
                "description": "Provide the customer ID."
            }
        },
        "required": [
            "Request",
            "Response",
            "CustomerId"
        ],
        "additionalProperties": false
    }
}`;

    return (
        <div>
            <div>Example of a JSON schema that defines the expected response structure:</div>
            <Code code={code} elementToCopy={code} language="json" />
        </div>
    );
};

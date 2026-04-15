import Code from "components/common/Code";
import { FormInput, FormGroup, FormLabel } from "components/common/Form";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";

interface EditAiAgentActionToolItemProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
}

export default function EditAiAgentActionToolItem({ index, remove, save, edit }: EditAiAgentActionToolItemProps) {
    const { control, setValue, trigger } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const handleSave = async () => {
        const isValid = await trigger([`actions.${index}`]);
        if (!isValid) {
            return;
        }
        save();
    };

    const actionItem = formValues.actions[index];

    if (!actionItem.isEditing) {
        return (
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center gap-3">
                <div className="tool-info">
                    <h4 className="m-0">{actionItem.name}</h4>
                    <small className="tool-description">{actionItem.description}</small>
                </div>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={edit}>
                        <Icon icon="chevron-down" margin="m-0" />
                    </Button>
                </div>
            </div>
        );
    }

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">Configure action tool</h4>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={handleSave}>
                        <Icon icon="chevron-up" margin="m-0" />
                    </Button>
                </div>
            </div>
            <hr className="mt-2 mb-3" />
            <FormGroup>
                <FormLabel>Tool name</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name={`actions.${index}.name`}
                    placeholder="e.g. ContactSupportTeam"
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Description</FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name={`actions.${index}.description`}
                    placeholder={actionFieldDescriptionPlaceholder}
                    rows={4}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName={`actions.${index}.parametersSampleObject`}
                sampleObjectLabel="Sample parameters object"
                sampleObject={actionItem.parametersSampleObject}
                sampleObjectSyntaxHelp={<ActionFieldSampleObjectSyntaxHelp />}
                sampleObjectTooltip={<ActionFieldSampleObjectTooltip />}
                sampleObjectPlaceholder={actionFieldSampleObjectPlaceholder}
                jsonSchemaName={`actions.${index}.parametersSchema`}
                jsonSchemaLabel="Parameters JSON schema"
                jsonSchema={actionItem.parametersSchema}
                jsonSchemaSyntaxHelp={<ActionFieldJsonSchemaSyntaxHelp />}
                schemaType="ToolParameters"
                helpActionTooltipTitle="Syntax example"
                canRegenerateSchemaName={`actions.${index}.canRegenerateSchema`}
            />
        </div>
    );
}

const actionFieldDescriptionPlaceholder = `In this description, explain to the LLM under which conditions it should trigger this action.
E.g., Trigger this action tool when you identify the customer has a problem with an order.`;

const actionFieldSampleObjectPlaceholder = `{
    // "FieldName": "Instruction to the LLM",
    // ...
    // "FieldName" is a custom field that you want the LLM to provide when triggering the action.
    // The value ("Instruction to the LLM") is a natural-language instruction that tells the LLM what content to provide in this field.
}
Open the (?) icon to view an example.`;

function ActionFieldSampleObjectTooltip() {
    return (
        <>
            This JSON object defines the format in which the LLM will supply data for the requested action when it
            decides to trigger this action tool.
            <br />
            The LLM will fill in values for the specified fields based on the conversation context and any relevant data
            it has access to.
            <br />
            <br />
            You can then pass this data to your client or backend to handle the requested task. After the task is
            completed, you should reply to the LLM with a free-text message describing the result of the action.
            <br />
            <br />
            This object is not sent to the model directly - RavenDB uses it to generate a JSON schema, which is sent to
            the model.
            <br />
            This object is optional. Providing an empty object <code>{"{}"}</code> is also valid if you don&apos;t need
            any data from the LLM.
        </>
    );
}

function ActionFieldSampleObjectSyntaxHelp() {
    const exampleCode = `{
    "customerId": "Fill in the ID of the customer who reported the problem.",
    "orderId": "Fill in the ID of the order the customer is complaining about.",
    "problemDescription": "Provide a description of the issue."
}`;

    const exampleCodeNoParameters = `{}`;

    return (
        <div>
            <div>
                This example shows a sample object you can define to specify the data format the LLM should provide when
                it decides to trigger this action tool.
                <br />
                <br />
                The values the LLM will supply can be based on the ongoing conversation or on data retrieved using query
                tools.
            </div>
            <Code code={exampleCode} language="json" />
            <div className="mt-2">Example 2 (no parameters will be used):</div>
            <Code code={exampleCodeNoParameters} language="json" />
        </div>
    );
}

function ActionFieldJsonSchemaSyntaxHelp() {
    const exampleCode = `{
    "type": "object",
    "properties": {
        "customerId": {
            "type": "string",
            "description": "Fill in the ID of the customer who reported the problem."
        },
        "orderId": {
            "type": "string",
            "description": "Fill in the ID of the order the customer is complaining about."
        },
        "problemDescription": {
            "type": "string",
            "description": "Provide a description of the issue."
        }
    },
    "required": [
        "customerId",
        "orderId",
        "problemDescription"
    ],
    "additionalProperties": false
}`;

    return (
        <div>
            <Code code={exampleCode} language="json" />
        </div>
    );
}

import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useFormContext, useWatch, useFieldArray } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import AceEditor from "components/common/ace/AceEditor";
import { FormInput, FormAceEditor } from "components/common/Form";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import useRqlLanguageService from "components/hooks/useRqlLanguageService";
import { useRef } from "react";
import ReactAce from "react-ace";
import { FormGroup, FormLabel } from "components/common/Form";
import queryCriteria from "models/database/query/queryCriteria";
import savedQueriesStorage from "common/storage/savedQueriesStorage";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Code from "components/common/Code";

export default function EditAiAgentToolsSection() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const queriesFieldArray = useFieldArray({
        name: "queries",
        control,
    });

    const actionsFieldArray = useFieldArray({
        name: "actions",
        control,
    });

    return (
        <>
            <h3 className="m-0 mt-3">Define agent tools</h3>
            <div className="mb-1">
                Tools are a controlled way to pass context to the LLM. Configure the tools that the LLM can instruct the
                agent to trigger in response to user prompts.
                <br />
                These include query tools (to retrieve data from the database) and action tools (to initiate tasks that
                are expected to be carried out by the client or user).
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <div className="hstack justify-content-between">
                    <div className="hstack gap-2">
                        <div className="p-1 rounded-2 bg-faded-primary border border-primary">
                            <Icon icon="query" color="primary" margin="m-0" />
                        </div>
                        <div>
                            Query tools
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        Define queries the agent is allowed to execute against the database in order to
                                        retrieve data.
                                        <br />
                                        <br />
                                        The LLM can instruct the agent to run these queries as needed to answer user
                                        questions.
                                        <br />
                                        <br />
                                        You can restrict the query scope by filtering results using the defined
                                        &quot;agent parameters&quot;.
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </div>
                    </div>
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        onClick={() =>
                            queriesFieldArray.append({
                                name: "",
                                description: "",
                                query: "",
                                parametersSchema: "",
                                isSaved: false,
                                isEditing: true,
                            })
                        }
                    >
                        <Icon icon="plus" />
                        Add new query tool
                    </Button>
                </div>
                <div className="vstack">
                    {queriesFieldArray.fields.map((field, index) => (
                        <QueryField
                            key={field.id}
                            index={index}
                            remove={() => queriesFieldArray.remove(index)}
                            save={() =>
                                queriesFieldArray.update(index, {
                                    ...formValues.queries[index],
                                    isSaved: true,
                                    isEditing: false,
                                })
                            }
                            edit={() =>
                                queriesFieldArray.update(index, { ...formValues.queries[index], isEditing: true })
                            }
                            cancelEdit={() => {
                                if (formValues.queries[index].isSaved) {
                                    queriesFieldArray.update(index, { ...formValues.queries[index], isEditing: false });
                                } else {
                                    queriesFieldArray.remove(index);
                                }
                            }}
                        />
                    ))}
                </div>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary mt-2">
                <div className="hstack justify-content-between">
                    <div className="hstack gap-2">
                        <div className="p-1 rounded-2 bg-faded-primary border border-primary">
                            <Icon icon="force" color="primary" margin="m-0" />
                        </div>
                        <div>
                            Action tools
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        Define actions that the agent can trigger when requested by the LLM, allowing
                                        the backend or client to perform operations in response to user prompts and
                                        conversation context.
                                        <br />
                                        <br />
                                        Each action tool should handle a specific task in your system - for example,
                                        creating a support ticket, sending a notification, or updating a document
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </div>
                    </div>
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        onClick={() =>
                            actionsFieldArray.append({
                                name: "",
                                description: "",
                                parametersSchema: "",
                                isSaved: false,
                                isEditing: true,
                            })
                        }
                    >
                        <Icon icon="plus" />
                        Add new action tool
                    </Button>
                </div>
                <div className="vstack">
                    {actionsFieldArray.fields.map((field, index) => (
                        <ActionField
                            key={field.id}
                            index={index}
                            remove={() => actionsFieldArray.remove(index)}
                            save={() =>
                                actionsFieldArray.update(index, {
                                    ...formValues.actions[index],
                                    isSaved: true,
                                    isEditing: false,
                                })
                            }
                            edit={() =>
                                actionsFieldArray.update(index, { ...formValues.actions[index], isEditing: true })
                            }
                            cancelEdit={() => {
                                if (formValues.actions[index].isSaved) {
                                    actionsFieldArray.update(index, { ...formValues.actions[index], isEditing: false });
                                } else {
                                    actionsFieldArray.remove(index);
                                }
                            }}
                        />
                    ))}
                </div>
            </div>
        </>
    );
}

interface QueryFieldProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
    cancelEdit: () => void;
}

function QueryField({ index, remove, save, edit, cancelEdit }: QueryFieldProps) {
    const { control, setValue, trigger } = useFormContext<EditAiAgentFormData>();
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);

    const queryAceRef = useRef<ReactAce>(null);

    const formValues = useWatch({
        control,
    });

    const handleSave = async () => {
        const isValid = await trigger([`queries.${index}`]);
        if (!isValid) {
            return;
        }
        save();
    };

    const rqlLanguageService = useRqlLanguageService();

    const queryItem = formValues.queries[index];

    if (!queryItem.isEditing) {
        return (
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center">
                <div>
                    <h4 className="m-0">{queryItem.name}</h4>
                    <small>{queryItem.description}</small>
                </div>
                <div className="hstack gap-2">
                    <Button variant="secondary" onClick={edit}>
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </div>
            </div>
        );
    }

    const linkToQuery = () => {
        const query = queryCriteria.empty();
        const queryText = queryItem.query;

        const regexToFind$: RegExp = /\$\w+/g;
        const matches = queryText.match(regexToFind$) || [];
        const parameters: Record<string, string> = Object.fromEntries(matches.map((match) => [match, "undefined"]));

        query.queryText(queryText);
        query.queryParameters(parameters);
        query.name(queryText);
        query.recentQuery(true);
        const queryDto = query.toStorageDto();
        savedQueriesStorage.saveAndNavigate(dbName, queryDto, {
            newWindow: true,
        });
    };

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">Add new query tool</h4>
                <div className="hstack gap-2">
                    <Button variant="outline-secondary" onClick={cancelEdit}>
                        Cancel
                    </Button>
                    <Button variant="success" onClick={handleSave}>
                        <Icon icon="save" />
                        Save
                    </Button>
                </div>
            </div>
            <hr />
            <FormGroup>
                <FormLabel>Tool name</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name={`queries.${index}.name`}
                    placeholder="e.g. GetOrdersByCountryAndCompany"
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Description</FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name={`queries.${index}.description`}
                    placeholder={queryFieldDescriptionPlaceholder}
                />
            </FormGroup>
            <FormGroup>
                <div className="d-flex mb-1 justify-content-between">
                    <FormLabel className="mb-0">Query</FormLabel>
                    <Button
                        variant="link"
                        className="m-0 p-0"
                        onClick={linkToQuery}
                        title="Click to test this query in the Studio's Query View"
                    >
                        Test query
                    </Button>
                </div>
                <FormAceEditor
                    aceRef={queryAceRef}
                    control={control}
                    name={`queries.${index}.query`}
                    mode="rql"
                    languageService={rqlLanguageService}
                    actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                    placeholder={queryFieldQueryPlaceholder}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName={`queries.${index}.parametersSampleObject`}
                sampleObjectLabel="Sample parameters object"
                sampleObject={queryItem.parametersSampleObject}
                sampleObjectSyntaxHelp={<QueryFieldQuerySyntaxHelp />}
                sampleObjectTooltip={<QueryFieldSampleObjectTooltip />}
                sampleObjectPlaceholder={queryFieldSampleObjectPlaceholder}
                jsonSchemaName={`queries.${index}.parametersSchema`}
                jsonSchemaLabel="Parameters JSON schema"
                jsonSchema={queryItem.parametersSchema}
                jsonSchemaSyntaxHelp={<QueryFieldJsonSchemaSyntaxHelp />}
                schemaType="ToolParameters"
                helpActionTooltipTitle="Syntax example"
                canRegenerateSchemaName={`queries.${index}.canRegenerateSchema`}
            />
        </div>
    );
}

function QueryFieldSampleObjectTooltip() {
    return (
        <>
            Provide a JSON object that defines the parameters the LLM is expected to supply when it requests the agent
            to execute this query tool.
            <br />
            <br />
            This object is not sent to the model directly - RavenDB uses it to generate a JSON schema, which is sent to
            the model.
        </>
    );
}

const queryFieldDescriptionPlaceholder = `In this description, explain to the LLM when it should trigger this query.
Example 1: Use this query to retrieve Order documents from the database filtered by destination country and company.
Example 2: Use this query to perform a semantic search for products similar to those in the customer's order.`;

const queryFieldQueryPlaceholder = `// Enter the query that will run against the database. 
// You can query an existing static index or make a dynamic query. For example:
// Example 1: from "Orders" where ShipTo.Country == $country" and Company == $company"
// Example 2: from "Products" where vector.search(embedding.text(Name), $searchTerm, $similarityLevel)

// Parameters (i.e. $paramName) that are defined in the "Set agent parameters" section will be replaced with the fixed values you provide.
// Other parameters (i.e. $paramName) will be filled with the values provided by the LLM when it calls this query tool.`;

const queryFieldSampleObjectPlaceholder = `{
    // "ParamName": "Instruction to the LLM",
    // ... 
    // "ParamName" is the name of a parameter from the query for which the LLM needs to provide a value.
    // The value ("Instruction to the LLM") is a natural-language instruction that tells the LLM what value to supply in this field.
}
Open the (?) icon to view an example.`;

const QueryFieldQuerySyntaxHelp = () => {
    const exampleCode1 = `{
    "country": "Provide the country to which the order was shipped.",
    "company": "Provide the company that placed this order."         
}`;

    const exampleCode2 = `{
    "searchTerm": "Provide the name of a product to search for similar items.",
    "similarityLevel": "Provide the similarity level to apply in the search."
}`;

    return (
        <div>
            <div>Example 1:</div>
            <Code code={exampleCode1} elementToCopy={exampleCode1} language="json" />
            <div className="mt-2">Example 2:</div>
            <Code code={exampleCode2} elementToCopy={exampleCode2} language="json" />
        </div>
    );
};

const QueryFieldJsonSchemaSyntaxHelp = () => {
    const exampleCode = `{
    "type": "object",
    "properties": {
        "country": {
            "type": "string",
            "description": "Provide the country to which the order was shipped."
        },
        "company": {
            "type": "string",
            "description": "Provide the company that placed this order."
        }
    },
    "required": [
        "country",
        "company"
    ],
    "additionalProperties": false
}`;

    return (
        <div>
            <div>Example:</div>
            <Code code={exampleCode} elementToCopy={exampleCode} language="json" />
        </div>
    );
};

interface ActionFieldProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
    cancelEdit: () => void;
}

function ActionField({ index, remove, save, edit, cancelEdit }: ActionFieldProps) {
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
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center">
                <div>
                    <h4 className="m-0">{actionItem.name}</h4>
                    <small>{actionItem.description}</small>
                </div>
                <div className="hstack gap-2">
                    <Button variant="secondary" onClick={edit}>
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </div>
            </div>
        );
    }

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">Add new action tool</h4>
                <div className="hstack gap-2">
                    <Button variant="outline-secondary" onClick={cancelEdit}>
                        Cancel
                    </Button>
                    <Button variant="success" onClick={handleSave}>
                        <Icon icon="save" />
                        Save
                    </Button>
                </div>
            </div>
            <hr />
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
            <Code code={exampleCode} elementToCopy={exampleCode} language="json" />
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
            <Code code={exampleCode} elementToCopy={exampleCode} language="json" />
        </div>
    );
}

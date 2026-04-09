import savedQueriesStorage from "common/storage/savedQueriesStorage";
import AceEditor from "components/common/ace/AceEditor";
import Code from "components/common/Code";
import { FormInput, FormAceEditor, FormGroup, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useRqlLanguageService from "components/hooks/useRqlLanguageService";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import queryCriteria from "models/database/query/queryCriteria";
import { useRef } from "react";
import ReactAce from "react-ace";
import Button from "react-bootstrap/Button";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { SelectOption } from "components/common/select/Select";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Accordion from "react-bootstrap/Accordion";
import AccordionButton from "react-bootstrap/AccordionButton";

interface EditAiAgentQueryToolItemProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
}

export default function EditAiAgentQueryToolItem({ index, remove, save, edit }: EditAiAgentQueryToolItemProps) {
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
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center gap-3">
                <div className="tool-info">
                    <h4 className="m-0">{queryItem.name}</h4>
                    <small className="tool-description">{queryItem.description}</small>
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

    const linkToQuery = () => {
        const query = queryCriteria.empty();
        let queryText = "";

        const regexToFind$: RegExp = /\$\w+/g;
        const allMatches = queryItem.query.match(regexToFind$) || [];
        const uniqueMatches = [...new Set(allMatches)];

        if (uniqueMatches.length > 0) {
            queryText += uniqueMatches.map((x) => `${x} = null`).join("\n");
            queryText += "\n\n";
        }

        queryText += queryItem.query;

        query.queryText(queryText);
        query.recentQuery(true);
        query.skipRunOnInit(true);
        const queryDto = query.toStorageDto();
        savedQueriesStorage.saveAndNavigate(dbName, queryDto, {
            newWindow: true,
        });
    };

    const isAdvancedOpenByDefault = queryItem.isAllowModelQueriesOverride || queryItem.isAddToInitialContextOverride;

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">Configure query tool</h4>
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
                    rows={4}
                />
            </FormGroup>
            <FormGroup>
                <div className="d-flex mb-1 justify-content-between align-items-end">
                    <FormLabel className="mb-0">Query</FormLabel>
                    {queryItem.query && (
                        <Button
                            variant="info"
                            className="rounded-pill"
                            onClick={linkToQuery}
                            title="Click to test this query in the Studio's Query View"
                        >
                            <Icon icon="rocket" />
                            Test query
                        </Button>
                    )}
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

            <Accordion defaultActiveKey={isAdvancedOpenByDefault ? "advanced-settings" : null} className="mt-2">
                <Accordion.Item eventKey="advanced-settings" className="border border-secondary rounded-2 panel-bg-2">
                    <Accordion.Header
                        as={() => <AccordionButton className="rounded-2 panel-bg-2">Advanced settings</AccordionButton>}
                    ></Accordion.Header>
                    <Accordion.Body>
                        <hr className="my-0 mb-2" />
                        <FormGroup>
                            <FormLabel>
                                Allow model queries
                                <PopoverWithHoverWrapper message={<AllowModelQueriesTooltip />}>
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <div className="d-flex flex-wrap align-items-center">
                                <FormSelect
                                    control={control}
                                    name={`queries.${index}.isAllowModelQueries`}
                                    options={isAllowModelQueriesOptions}
                                    isDisabled={!queryItem.isAllowModelQueriesOverride}
                                    placeholder={
                                        queryItem.isAllowModelQueriesOverride ? "Select True or False" : "Default"
                                    }
                                />
                                <FormSwitch
                                    control={control}
                                    name={`queries.${index}.isAllowModelQueriesOverride`}
                                    className="ms-2"
                                    afterChange={(isChecked) => {
                                        if (!isChecked) {
                                            setValue(`queries.${index}.isAllowModelQueries`, null, {
                                                shouldValidate: true,
                                            });
                                        }
                                    }}
                                >
                                    Override
                                </FormSwitch>
                            </div>
                        </FormGroup>
                        <FormGroup className="mb-0">
                            <FormLabel>
                                Add to initial context
                                <PopoverWithHoverWrapper message={<AddToInitialContextTooltip />}>
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <div className="d-flex flex-wrap align-items-center">
                                <FormSelect
                                    control={control}
                                    name={`queries.${index}.isAddToInitialContext`}
                                    options={isAddToInitialContextOptions}
                                    isDisabled={!queryItem.isAddToInitialContextOverride}
                                    placeholder={
                                        queryItem.isAddToInitialContextOverride ? "Select True or False" : "Default"
                                    }
                                />
                                <FormSwitch
                                    control={control}
                                    name={`queries.${index}.isAddToInitialContextOverride`}
                                    className="ms-2"
                                    afterChange={(isChecked) => {
                                        if (!isChecked) {
                                            setValue(`queries.${index}.isAddToInitialContext`, null, {
                                                shouldValidate: true,
                                            });
                                        }
                                    }}
                                >
                                    Override
                                </FormSwitch>
                            </div>
                        </FormGroup>
                    </Accordion.Body>
                </Accordion.Item>
            </Accordion>
        </div>
    );
}

function AllowModelQueriesTooltip() {
    return (
        <>
            When True, the model is allowed to execute this query on demand based on its own judgment.
            <br />
            <br />
            When False, the model cannot call this query (unless executed as part of initial context).
            <br />
            <br />
            When Default, server default is applied.
        </>
    );
}

function AddToInitialContextTooltip() {
    return (
        <>
            When True, the query will be executed during the initial context build and its results provided to the
            model.
            <br />
            <br />
            When False, the query will not be executed for the initial context.
            <br />
            <br />
            When Default, server default is applied.
        </>
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

    const exampleCode3 = `{}`;

    return (
        <div>
            <div>Example 1:</div>
            <Code code={exampleCode1} language="json" />
            <div className="mt-2">Example 2:</div>
            <Code code={exampleCode2} language="json" />
            <div className="mt-2">Example 3 (no parameters will be used):</div>
            <Code code={exampleCode3} language="json" />
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
            <Code code={exampleCode} language="json" />
        </div>
    );
};

const isAllowModelQueriesOptions: SelectOption<boolean>[] = [
    { label: "True", value: true },
    { label: "False", value: false },
];

const isAddToInitialContextOptions: SelectOption<boolean>[] = [
    { label: "True", value: true },
    { label: "False", value: false },
];

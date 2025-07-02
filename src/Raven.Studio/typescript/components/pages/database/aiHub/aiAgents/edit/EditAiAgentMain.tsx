import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    FormAceEditor,
    FormDurationPicker,
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSelectAutocomplete,
} from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import { useAppSelector } from "components/store";
import { sortBy } from "lodash";
import { useAsync } from "react-async-hook";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "./utils/editAiAgentValidation";
import InputGroup from "react-bootstrap/InputGroup";
import useBoolean from "components/hooks/useBoolean";
import { useRef } from "react";
import ReactAce from "react-ace/lib/ace";
import AceEditor from "components/common/ace/AceEditor";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Table from "react-bootstrap/Table";
import { EmptySet } from "components/common/EmptySet";

export default function EditAiAgentMain() {
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const queriesFieldArray = useFieldArray({
        name: "queries",
        control,
    });

    const systemPromptRef = useRef<ReactAce>(null);
    const outputSchemaRef = useRef<ReactAce>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { tasksService } = useServices();

    const { value: isNewConnectionStringOpen, toggle: toggleIsNewConnectionStringOpen } = useBoolean(false);

    const asyncGetConnectionStringsOptions = useAsync(async () => {
        const result = await tasksService.getConnectionStrings(databaseName);

        const connectionStrings = Object.values(result.AiConnectionStrings).map((x) => x.Name);

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

    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    return (
        <>
            <h3 className="m-0">Configure basic settings</h3>
            <div className="mb-1">
                Setup basic information about your agent - give it a specific task, database it will connect to and
                format in which agent will respond.
            </div>
            <div className="panel-bg-1 p-2 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>Name</FormLabel>
                    <FormInput type="text" control={control} name="name" placeholder="e.g. Customer Service Agent" />
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
                <FormGroup>
                    <FormLabel>Output schema</FormLabel>
                    <FormAceEditor
                        aceRef={outputSchemaRef}
                        control={control}
                        name="outputSchema"
                        mode="json"
                        actions={[
                            { component: <AceEditor.FullScreenAction /> },
                            { component: <AceEditor.FormatAction /> },
                        ]}
                    />
                </FormGroup>
            </div>
            <h3 className="m-0 mt-3">Set chat persistence</h3>
            <div className="mb-1">TODO</div>
            <div className="panel-bg-1 p-2 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>Collection name</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name="persistenceCollectionName"
                        options={collectionOptions}
                    />
                </FormGroup>
                <FormGroup>
                    <FormLabel>Expire in</FormLabel>
                    <FormDurationPicker control={control} name="persistenceExpires" showDays />
                </FormGroup>
            </div>
            <h3 className="m-0 mt-3">Define agent tools</h3>
            <div className="mb-1">
                Define tool queries to let AI retrieve data, and tool actions to let perform tasks.
            </div>
            <div className="panel-bg-1 p-2 rounded-2 border border-secondary">
                <div className="hstack justify-content-between">
                    <div className="hstack gap-2">
                        <div className="p-1 rounded-2 bg-faded-primary border border-primary">
                            <Icon icon="query" color="primary" margin="m-0" />
                        </div>
                        Tool queries
                    </div>
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        onClick={() =>
                            queriesFieldArray.append({
                                name: "",
                                description: "",
                                query: "",
                                parametersSchema: [],
                                isSaved: false,
                            })
                        }
                    >
                        <Icon icon="plus" />
                        Add new
                    </Button>
                </div>
                <div className="vstack">
                    {queriesFieldArray.fields.map((field, index) => (
                        <QueryField
                            key={field.id}
                            index={index}
                            remove={() => queriesFieldArray.remove(index)}
                            save={() =>
                                queriesFieldArray.update(index, { ...formValues.queries[index], isSaved: true })
                            }
                            edit={() =>
                                queriesFieldArray.update(index, { ...formValues.queries[index], isEditing: true })
                            }
                            cancelEdit={() =>
                                queriesFieldArray.update(index, { ...formValues.queries[index], isEditing: false })
                            }
                        />
                    ))}
                </div>
            </div>
        </>
    );
}

function QueryField({
    index,
    remove,
    save,
    edit,
    cancelEdit,
}: {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
    cancelEdit: () => void;
}) {
    const { control } = useFormContext<EditAiAgentFormData>();

    const queryAceRef = useRef<ReactAce>(null);

    const parametersFieldArray = useFieldArray({
        name: `queries.${index}.parametersSchema`,
        control,
    });

    const formValues = useWatch({
        control,
    });

    const queryItem = formValues.queries[index];

    if (queryItem.isSaved) {
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

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">Add new tool</h4>
                <div className="hstack gap-2">
                    <Button variant="outline-secondary" onClick={cancelEdit}>
                        Cancel
                    </Button>
                    <Button variant="info">
                        <Icon icon="test" />
                        Test
                    </Button>
                    <Button variant="success" onClick={save}>
                        <Icon icon="save" />
                        Save
                    </Button>
                </div>
            </div>
            <hr />
            <FormGroup>
                <FormLabel>Name</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name={`queries.${index}.name`}
                    placeholder="e.g. GetCustomerInfo"
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Description</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name={`queries.${index}.description`}
                    placeholder="e.g. Get details about a customer by ID"
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Query</FormLabel>
                <FormAceEditor
                    aceRef={queryAceRef}
                    control={control}
                    name={`queries.${index}.query`}
                    mode="text" // TODO RQL
                    actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel className="hstack justify-content-between">
                    Parameters schema
                    <Button
                        variant="link"
                        size="sm"
                        onClick={() => parametersFieldArray.append({ parameter: "", description: "" })}
                    >
                        <Icon icon="plus" />
                        Add new
                    </Button>
                </FormLabel>
                {parametersFieldArray.fields.length === 0 && (
                    <div className="panel-bg-1 p-2 rounded-2 border border-secondary d-flex justify-content-center align-items-center">
                        <EmptySet compact>No parameters added</EmptySet>
                    </div>
                )}
                {parametersFieldArray.fields.length > 0 && (
                    <Table bordered striped className="parameters-table">
                        <thead>
                            <tr>
                                <th>Parameter</th>
                                <th>Description</th>
                                <th style={{ width: "70px" }}></th>
                            </tr>
                        </thead>
                        <tbody>
                            {parametersFieldArray.fields.map((field, index) => (
                                <tr key={field.id}>
                                    <td>
                                        <FormInput
                                            type="text"
                                            control={control}
                                            name={`queries.${index}.parametersSchema.${index}.parameter`}
                                        />
                                    </td>
                                    <td>
                                        <FormInput
                                            type="text"
                                            control={control}
                                            name={`queries.${index}.parametersSchema.${index}.description`}
                                        />
                                    </td>
                                    <td>
                                        <Button
                                            variant="link"
                                            onClick={() => parametersFieldArray.remove(index)}
                                            className="text-danger"
                                        >
                                            <Icon icon="trash" margin="m-0" />
                                        </Button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </Table>
                )}
            </FormGroup>
        </div>
    );
}

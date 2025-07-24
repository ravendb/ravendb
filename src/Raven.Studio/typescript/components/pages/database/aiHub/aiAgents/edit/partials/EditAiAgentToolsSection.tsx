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
                Define tool queries to let AI retrieve data, and tool actions to let perform tasks.
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
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
                                parametersSchema: "",
                                isSaved: false,
                                isEditing: true,
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
                        Tool actions
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
                        Add new
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

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">Add new tool</h4>
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
                    type="textarea"
                    as="textarea"
                    control={control}
                    name={`queries.${index}.description`}
                    placeholder="e.g. Get details about a customer by ID"
                    rows={4}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Query</FormLabel>
                <FormAceEditor
                    aceRef={queryAceRef}
                    control={control}
                    name={`queries.${index}.query`}
                    mode="rql"
                    languageService={rqlLanguageService}
                    actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName={`queries.${index}.parametersSampleObject`}
                sampleObjectLabel="Parameters sample object"
                sampleObject={queryItem.parametersSampleObject}
                sampleObjectSyntaxHelp={<div>TODO</div>}
                jsonSchemaName={`queries.${index}.parametersSchema`}
                jsonSchemaLabel="Parameters JSON schema"
                jsonSchema={queryItem.parametersSchema}
                jsonSchemaSyntaxHelp={<div>TODO</div>}
            />
        </div>
    );
}

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
                <h4 className="m-0">Add new tool</h4>
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
                <FormLabel>Name</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name={`actions.${index}.name`}
                    placeholder="e.g. GetCustomerInfo"
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Description</FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name={`actions.${index}.description`}
                    placeholder="e.g. Get details about a customer by ID"
                    rows={4}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName={`actions.${index}.parametersSampleObject`}
                sampleObjectLabel="Parameters sample object"
                sampleObject={actionItem.parametersSampleObject}
                sampleObjectSyntaxHelp={<div>TODO</div>}
                jsonSchemaName={`actions.${index}.parametersSchema`}
                jsonSchemaLabel="Parameters JSON schema"
                jsonSchema={actionItem.parametersSchema}
                jsonSchemaSyntaxHelp={<div>TODO</div>}
            />
        </div>
    );
}

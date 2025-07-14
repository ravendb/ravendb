import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    FormAceEditor,
    FormDurationPicker,
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSelectAutocomplete,
    FormSwitch,
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
import { useMemo, useRef } from "react";
import ReactAce from "react-ace/lib/ace";
import AceEditor from "components/common/ace/AceEditor";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { LoadingView } from "components/common/LoadingView";
import rqlLanguageService from "common/rqlLanguageService";
import Badge from "react-bootstrap/Badge";
import { EmptySet } from "components/common/EmptySet";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import TaskUtils from "components/utils/TaskUtils";
import { editAiAgentSelectors } from "./store/editAiAgentSlice";

export default function EditAiAgentMain() {
    const { control, setValue, formState } = useFormContext<EditAiAgentFormData>();

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

    const systemPromptRef = useRef<ReactAce>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isDocumentExpirationEnabled = useAppSelector(editAiAgentSelectors.isDocumentExpirationEnabled);

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

    const handleGenerateIdentifier = () => {
        setValue("identifier", TaskUtils.getGeneratedIdentifier(formValues.name));
    };

    if (formState.isLoading) {
        return <LoadingView />;
    }

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
                    sampleObject={formValues.sampleObject}
                    sampleObjectSyntaxHelp={<div>TODO</div>}
                    jsonSchemaName="outputSchema"
                    jsonSchema={formValues.outputSchema}
                    jsonSchemaSyntaxHelp={<div>TODO</div>}
                />
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
                {isDocumentExpirationEnabled.status === "success" && !isDocumentExpirationEnabled.data && (
                    <FormGroup>
                        <FormSwitch control={control} name="isEnableDocumentExpiration">
                            Enable document expiration
                        </FormSwitch>
                    </FormGroup>
                )}
                {(formValues.isEnableDocumentExpiration || isDocumentExpirationEnabled.data) && (
                    <FormGroup>
                        <FormLabel>Expire in</FormLabel>
                        <FormDurationPicker control={control} name="persistenceExpiresInSeconds" showDays isFlexGrow />
                    </FormGroup>
                )}
            </div>
            <ParametersField />
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
            <div className="panel-bg-1 p-2 rounded-2 border border-secondary mt-2">
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

function ParametersField() {
    const { control, setValue, trigger } = useFormContext<EditAiAgentFormData>();

    const parametersFieldArray = useFieldArray({
        name: "parameters",
        control,
    });

    const formValues = useWatch({
        control,
    });

    const handleAddParameter = async () => {
        const isValid = await trigger("parameterInput");
        if (!isValid || !formValues.parameterInput) {
            return;
        }

        parametersFieldArray.append({ name: formValues.parameterInput });
        setValue("parameterInput", "");
    };

    return (
        <>
            <h3 className="m-0 mt-3">Set agent parameters</h3>
            <div className="mb-1">
                Create parameters to control and restrict data that you want your agent to have access to.
            </div>
            <div className="panel-bg-1 p-2 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>Define a parameter</FormLabel>
                    <FormInput
                        type="text"
                        control={control}
                        name="parameterInput"
                        placeholder="e.g. company"
                        addon={
                            <Button variant="link" className="text-reset" onClick={handleAddParameter}>
                                <Icon icon="plus" />
                                Add parameter
                            </Button>
                        }
                    />
                </FormGroup>
                <FormGroup>
                    <FormLabel>List of parameters</FormLabel>
                    {parametersFieldArray.fields.length === 0 ? (
                        <EmptySet compact className="text-muted">
                            No parameters have been defined yet
                        </EmptySet>
                    ) : (
                        <div className="d-flex gap-2 flex-wrap">
                            {parametersFieldArray.fields.map((field, index) => (
                                <Badge key={field.id} bg="primary" pill>
                                    {field.name}
                                    <Button
                                        variant="link"
                                        className="p-0"
                                        onClick={() => parametersFieldArray.remove(index)}
                                        size="xs"
                                    >
                                        <Icon icon="trash" margin="m-0" color="light" />
                                    </Button>
                                </Badge>
                            ))}
                        </div>
                    )}
                </FormGroup>
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
        const isValid = await trigger([`queries.${index}.query`]);
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
                    <Button variant="info">
                        <Icon icon="test" />
                        Test (TODO)
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
                    mode="rql"
                    languageService={rqlLanguageService}
                    actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName={`queries.${index}.parametersSampleObject`}
                sampleObject={queryItem.parametersSampleObject}
                sampleObjectSyntaxHelp={<div>TODO</div>}
                jsonSchemaName={`queries.${index}.parametersSchema`}
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
        const isValid = await trigger([`actions.${index}.parametersSchema`]);
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
                    <Button variant="info">
                        <Icon icon="test" />
                        Test
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
                    type="text"
                    control={control}
                    name={`actions.${index}.description`}
                    placeholder="e.g. Get details about a customer by ID"
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName={`actions.${index}.parametersSampleObject`}
                sampleObject={actionItem.parametersSampleObject}
                sampleObjectSyntaxHelp={<div>TODO</div>}
                jsonSchemaName={`actions.${index}.parametersSchema`}
                jsonSchema={actionItem.parametersSchema}
                jsonSchemaSyntaxHelp={<div>TODO</div>}
            />
        </div>
    );
}

function useRqlLanguageService(): rqlLanguageService {
    const db = useAppSelector(databaseSelectors.activeDatabase);

    const { databasesService } = useServices();

    const asyncGetIndexNames = useAsync(async () => {
        const dto = await databasesService.getEssentialStats(db.name);
        return dto?.Indexes?.map((x) => x.Name);
    }, []);

    const languageService = useMemo(
        () => new rqlLanguageService(db, () => asyncGetIndexNames.result ?? [], "Select"),
        [asyncGetIndexNames.result, db]
    );

    return languageService;
}

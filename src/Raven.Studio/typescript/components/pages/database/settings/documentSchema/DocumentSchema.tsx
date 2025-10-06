import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import { AboutViewHeading } from "components/common/AboutView";
import { Checkbox } from "components/common/Checkbox";
import React, { useRef } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { SelectOption } from "components/common/select/Select";
import { HrHeader } from "components/common/HrHeader";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelSelect,
} from "components/common/RichPanel";
import { FormAceEditor, FormGroup, FormLabel, FormSelectCreatable } from "components/common/Form";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace/lib/ace";
import useBoolean from "hooks/useBoolean";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import jsonUtil from "common/jsonUtil";
import * as yup from "yup";
import { FormProvider, useForm, useFormContext } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { documentSchemaActions, DocumentSchemaValidatorConfig } from "./store/documentSchemaSlice";
import { documentSchemaSelectors } from "./store/documentSchemaSliceSelectors";
import { useServices } from "hooks/useServices";
import { useAsync, UseAsyncReturn } from "react-async-hook";
import DocumentSchemaSelectActions from "components/pages/database/settings/documentSchema/partials/DocumentSchemaSelectActions";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";
import DocumentSchemaAboutView from "components/pages/database/settings/documentSchema/partials/DocumentSchemaAboutView";
import DocumentSchemaDeleteModal from "components/pages/database/settings/documentSchema/partials/DocumentSchemaDeleteModal";
import DocumentSchemaOperationConfirm, {
    DocumentSchemaOperationConfirmType,
} from "components/pages/database/settings/documentSchema/partials/DocumentSchemaOperationConfirm";
import { EmptySet } from "components/common/EmptySet";
import { useDocumentSchema } from "components/pages/database/settings/documentSchema/useDocumentSchema";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import genUtils from "common/generalUtils";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import Code from "components/common/Code";
import DocumentSchemaFilter from "components/pages/database/settings/documentSchema/DocumentSchemaFilter";

export default function DocumentSchema() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const asyncLoadValidators = useAsync(async () => {
        const result = await databasesService.getSchemaValidation(databaseName);
        dispatch(documentSchemaActions.validatorsLoadedFromServer(result));
        return result;
    }, []);

    const {
        options,
        hasAnyValidator,
        filteredValidators,
        selectedCollections,
        handleOnSelectCollection,
        handleAddNew,
        selectedStatuses,
        setSelectedStatuses,
    } = useDocumentSchema();

    return (
        <div className="content-margin">
            <Col>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading marginBottom={4} title="Document Schema" icon="document-schema" />
                        {hasDatabaseAdminAccess && <DocumentSchemaSelectActions />}

                        {hasAnyValidator && (
                            <DocumentSchemaFilter
                                selectedCollections={selectedCollections}
                                setSelectedCollections={handleOnSelectCollection}
                                collectionOptions={options}
                                selectedStatuses={selectedStatuses}
                                setSelectedStatuses={setSelectedStatuses}
                                schemasCount={filteredValidators.length}
                                isLoading={asyncLoadValidators.loading}
                            />
                        )}

                        <div className="mt-4">
                            <HrHeader
                                count={filteredValidators.length}
                                right={
                                    hasDatabaseAdminAccess && (
                                        <Button
                                            size="xs"
                                            variant="info"
                                            className="rounded-pill"
                                            onClick={handleAddNew}
                                            title="Click to add a new schema for a collection"
                                        >
                                            <Icon icon="plus" />
                                            Add new
                                        </Button>
                                    )
                                }
                            >
                                <Icon icon="documents" />
                                <span>Collection specific document schemas</span>
                                <PopoverWithHoverWrapper message="Define and manage JSON Schemas for each collection">
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </HrHeader>
                            <DocumentSchemaBody
                                filteredValidators={filteredValidators}
                                asyncLoadValidators={asyncLoadValidators}
                            />
                        </div>
                    </Col>
                    <Col sm={12} lg={4}>
                        <DocumentSchemaAboutView />
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

interface DocumentSchemaBodyProps {
    asyncLoadValidators: UseAsyncReturn<Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration>;
    filteredValidators: DocumentSchemaValidatorConfig[];
}

const DocumentSchemaBody = ({ asyncLoadValidators, filteredValidators }: DocumentSchemaBodyProps) => {
    const { handleCancelNew, handleNewSchemaSubmit, draftIds, allCollectionNames } = useDocumentSchema();

    if (asyncLoadValidators.loading) {
        return <LoadingView />;
    }

    if (asyncLoadValidators.error) {
        return (
            <LoadError
                error="Failed to load document schemas. Please try again later."
                refresh={asyncLoadValidators.execute}
            />
        );
    }

    if (filteredValidators.length === 0 && draftIds.length === 0 && !asyncLoadValidators.loading) {
        return (
            <>
                <EmptySet>There are no collection specific document schemas.</EmptySet>
                {draftIds.map((id) => (
                    <NewCollectionSchemaRichPanel
                        key={id}
                        schemaValidatorCollections={allCollectionNames}
                        onSubmit={(data) => handleNewSchemaSubmit(id, data)}
                        onCancel={() => handleCancelNew(id)}
                    />
                ))}
            </>
        );
    }

    return (
        <>
            {draftIds.map((id) => (
                <NewCollectionSchemaRichPanel
                    key={id}
                    schemaValidatorCollections={allCollectionNames}
                    onSubmit={(data) => handleNewSchemaSubmit(id, data)}
                    onCancel={() => handleCancelNew(id)}
                />
            ))}
            {filteredValidators.map((v) => (
                <CollectionSchemaRichPanel
                    schemaValidatorCollections={allCollectionNames}
                    key={v.Name}
                    validator={v}
                    schema={genUtils.stringify(JSON.parse(v.Schema))}
                />
            ))}
        </>
    );
};

interface DocumentSchemaStatusProps {
    validator: DocumentSchemaValidatorConfig;
    canEdit: boolean;
    onStatusToggle: (disabled: boolean) => void;
    isTogglingState: boolean;
}

export function DocumentSchemaStatus({
    validator,
    canEdit,
    onStatusToggle,
    isTogglingState,
}: DocumentSchemaStatusProps) {
    return (
        <Dropdown>
            <Dropdown.Toggle
                disabled={!canEdit || isTogglingState}
                variant={validator.Disabled ? "warning" : "success"}
            >
                {isTogglingState && <Spinner size="sm" />} {validator.Disabled ? "Disabled" : "Enabled"}
            </Dropdown.Toggle>
            <Dropdown.Menu>
                <Dropdown.Item onClick={() => onStatusToggle(false)}>
                    <Icon icon="play" color="success" /> Enable
                </Dropdown.Item>
                <Dropdown.Item onClick={() => onStatusToggle(true)}>
                    <Icon icon="stop" color="danger" />
                    Disable
                </Dropdown.Item>
            </Dropdown.Menu>
        </Dropdown>
    );
}

interface CollectionSchemaRichPanelProps {
    validator: DocumentSchemaValidatorConfig;
    schema: string;
    schemaValidatorCollections: string[];
}

const CollectionSchemaRichPanel = ({
    validator,
    schema = "",
    schemaValidatorCollections,
}: CollectionSchemaRichPanelProps) => {
    const { value: isEditingSchema, toggle: toggleEditingSchema } = useBoolean(false);
    const { value: isTogglingStatus, setTrue: setTogglingStatus, setFalse: unsetTogglingStatus } = useBoolean(false);
    const [operationConfirm, setOperationConfirm] = React.useState<{
        type: DocumentSchemaOperationConfirmType;
        onConfirm: () => void;
        validators: DocumentSchemaValidatorConfig[];
    }>(null);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const dispatch = useAppDispatch();
    const isSelected = useAppSelector(documentSchemaSelectors.isSelectedCollectionName(validator.Name));
    const validatorsAll = useAppSelector(documentSchemaSelectors.allValidators);
    const { databasesService } = useServices();
    const { value: isDeleteModalOpen, toggle: toggleDeleteModal } = useBoolean(false);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const asyncSaveValidators = async (items: DocumentSchemaValidatorConfig[]) => {
        await databasesService.saveSchemaValidation(
            databaseName,
            documentSchemaUtils.mapToSchemaValidationConfigurationDto(items)
        );
        dispatch(documentSchemaActions.validatorsSaved());
    };

    const form = useForm<DocumentSchemaFormData>({
        resolver: yupResolver(formSchema),
        defaultValues: {
            collection: validator.Name,
            schema,
        },
    });

    const { handleSubmit, reset } = form;

    const handleEditSchema = async (data: DocumentSchemaFormData) => {
        const updatedItem = documentSchemaUtils.mapToDocumentSchemaValidatorConfigDto(data);
        dispatch(documentSchemaActions.validatorEdited({ originalName: validator.Name, validator: updatedItem }));
        const next = [...validatorsAll.filter((v) => v.Name !== validator.Name), updatedItem];
        await asyncSaveValidators(next);
        toggleEditingSchema();
    };

    const handleBulkStatusToggle = async (disabled: boolean) => {
        try {
            setTogglingStatus();
            const updatedValidator = { ...validator, Disabled: disabled };
            dispatch(
                documentSchemaActions.validatorEdited({ originalName: validator.Name, validator: updatedValidator })
            );
            const next = [...validatorsAll.filter((v) => v.Name !== validator.Name), updatedValidator];
            await asyncSaveValidators(next);
        } finally {
            unsetTogglingStatus();
        }
    };

    const handleStatusToggle = (disabled: boolean) => {
        const operationType: DocumentSchemaOperationConfirmType = disabled ? "disable" : "enable";

        setOperationConfirm({
            type: operationType,
            onConfirm: () => handleBulkStatusToggle(disabled),
            validators: [validator],
        });
    };

    return (
        <>
            <FormProvider {...form}>
                <RichPanel>
                    <RichPanelHeader>
                        <RichPanelInfo>
                            {hasDatabaseAdminAccess && (
                                <RichPanelSelect>
                                    <Checkbox
                                        toggleSelection={() =>
                                            dispatch(
                                                documentSchemaActions.selectedCollectionNameToggled(validator.Name)
                                            )
                                        }
                                        selected={isSelected}
                                    />
                                </RichPanelSelect>
                            )}
                            <RichPanelName>{validator.Name}</RichPanelName>
                        </RichPanelInfo>
                        {hasDatabaseAdminAccess && (
                            <RichPanelActions>
                                <DocumentSchemaStatus
                                    validator={validator}
                                    canEdit={hasDatabaseAdminAccess}
                                    onStatusToggle={handleStatusToggle}
                                    isTogglingState={isTogglingStatus}
                                />
                                <ButtonWithSpinner
                                    onClick={isEditingSchema ? handleSubmit(handleEditSchema) : toggleEditingSchema}
                                    variant={isEditingSchema ? "success" : "secondary"}
                                    isSpinning={form.formState.isSubmitting}
                                    icon={isEditingSchema ? "save" : "edit"}
                                >
                                    {isEditingSchema ? <span className="ms-1">Save</span> : null}
                                </ButtonWithSpinner>
                                {isEditingSchema ? (
                                    <Button
                                        variant="secondary"
                                        disabled={form.formState.isSubmitting}
                                        onClick={() => {
                                            reset({ collection: validator.Name, schema });
                                            toggleEditingSchema();
                                        }}
                                    >
                                        <Icon icon="close" />
                                        Discard
                                    </Button>
                                ) : (
                                    <ButtonWithSpinner
                                        isSpinning={form.formState.isSubmitting}
                                        variant="danger"
                                        onClick={toggleDeleteModal}
                                        icon="trash"
                                    />
                                )}
                            </RichPanelActions>
                        )}
                    </RichPanelHeader>

                    {isEditingSchema && (
                        <RichPanelDetailsEditSchema
                            schemaValidatorCollections={schemaValidatorCollections}
                            collectionName={validator.Name}
                        />
                    )}
                    {!isEditingSchema && <RichPanelDetailsViewSchema schema={schema} />}
                </RichPanel>
            </FormProvider>
            {isDeleteModalOpen && (
                <DocumentSchemaDeleteModal collectionName={validator.Name} onHide={toggleDeleteModal} />
            )}
            {operationConfirm && (
                <DocumentSchemaOperationConfirm
                    type={operationConfirm.type}
                    validators={operationConfirm.validators}
                    toggle={() => setOperationConfirm(null)}
                    onConfirm={operationConfirm.onConfirm}
                />
            )}
        </>
    );
};

interface RichPanelDetailsProps {
    schema?: string;
}

const RichPanelDetailsViewSchema = ({ schema }: RichPanelDetailsProps) => {
    const aceRef = useRef<ReactAce>(null);

    return (
        <RichPanelDetails>
            <FormGroup className="w-100 mt-2">
                <FormLabel>Document schema (Read only)</FormLabel>
                <AceEditor
                    readOnly
                    aceRef={aceRef}
                    isFullScreenLabelHidden
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        {
                            component: <AceEditor.HelpAction message={<ScriptSyntaxHelp />} />,
                            position: "bottom",
                        },
                    ]}
                    mode="json"
                    value={genUtils.stringify(JSON.parse(schema)) ?? ""}
                />
            </FormGroup>
        </RichPanelDetails>
    );
};

function ScriptSyntaxHelp() {
    const employeeSchema = `
{
    "title": "Employee",
    "type": "object",
    "properties": {
        "FirstName": { "type": "string" },
        "LastName": { "type": "string" },
        "Title": { "type": "string" },
        "Address": {
            "type": "object",
            "properties": {
                "City": { "type": "string" },
                "Country": { "type": "string" },
                "Line1": { "type": "string" },
                "Line2": { "type": ["string", "null"] },
                "Location": {
                    "type": "object",
                    "properties": {
                        "Latitude": { "type": "number" },
                        "Longitude": { "type": "number" }
                    }
                },
                "PostalCode": { "type": "string" },
                "Region": { "type": "string" }
            },
            "required": ["City", "Country", "Line1", "PostalCode", "Region"]
        },
        "Birthday": { "type": "string", "format": "date-time" },
        "HiredAt": { "type": "string", "format": "date-time" },
        "ReportsTo": {
            "type": ["string", "null"],
            "pattern": "^employees/\\\\d+-[A-Z]$"
        },
        "HomePhone": {
            "type": ["string", "null"],
            "pattern": "^\\\\(\\\\d{1,3}\\\\)\\\\s?\\\\d{3}-\\\\d{4}$",
            "description": "Phone number in the format (206) 555-1189 or (71) 555-4848."
        },
        "Notes": {
            "type": "array",
            "items": {
                "type": "string",
                "minLength": 10,
                "maxLength": 1000
            },
            "maxItems": 10
        }
    },
    "required": ["FirstName", "LastName", "Title", "Address", "HomePhone"],
    "additionalProperties": true
}`;

    return (
        <div>
            <div>
                Sample schema for a document in the <code>Employees</code> collection:
            </div>
            <Code code={employeeSchema} language="javascript" elementToCopy={employeeSchema} />
        </div>
    );
}

interface RichPanelDetailsEditSchemaProps extends RichPanelDetailsProps {
    collectionName: string;
    schemaValidatorCollections: string[];
}

const RichPanelDetailsEditSchema = ({
    collectionName,
    schemaValidatorCollections,
}: RichPanelDetailsEditSchemaProps) => {
    const { control, setValue } = useFormContext<DocumentSchemaFormData>();
    const aceRef = useRef<ReactAce>(null);

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo" && x !== collectionName && !schemaValidatorCollections.includes(x)
    );

    const collectionOptions: SelectOption[] = allCollectionNames.map((x) => ({
        label: x,
        value: x,
    }));

    return (
        <RichPanelDetails>
            <FormGroup className="w-100 mt-2">
                <FormGroup>
                    <FormLabel>Collection</FormLabel>
                    <FormSelectCreatable
                        control={control}
                        name="collection"
                        placeholder="Select a collection (or enter a new one)"
                        options={collectionOptions}
                    />
                </FormGroup>
                <FormLabel title={`Edit the JSON schema for documents in the collection`}>
                    Document schema <Icon icon="info" color="info" margin="m-0" />
                </FormLabel>
                <FormAceEditor
                    control={control}
                    name="schema"
                    height="400px"
                    aceRef={aceRef}
                    isFullScreenLabelHidden
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        {
                            component: (
                                <AceEditor.LoadFileAction
                                    onLoad={(value) =>
                                        setValue("schema", value, {
                                            shouldValidate: true,
                                        })
                                    }
                                />
                            ),
                        },
                        {
                            component: <AceEditor.HelpAction message={<ScriptSyntaxHelp />} />,
                            position: "bottom",
                        },
                    ]}
                    mode="json"
                />
            </FormGroup>
        </RichPanelDetails>
    );
};

interface NewCollectionSchemaRichPanelProps {
    schemaValidatorCollections: string[];
    onSubmit: (data: DocumentSchemaFormData) => Promise<void>;
    onCancel: () => void;
}

const NewCollectionSchemaRichPanel = ({
    schemaValidatorCollections,
    onSubmit,
    onCancel,
}: NewCollectionSchemaRichPanelProps) => {
    const form = useForm<DocumentSchemaFormData>({
        resolver: yupResolver(formSchema),
    });

    const handleSubmit = async (data: DocumentSchemaFormData) => {
        await onSubmit(data);
    };

    return (
        <FormProvider {...form}>
            <RichPanel>
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>New Collection Schema</RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        <Button onClick={form.handleSubmit(handleSubmit)} variant="success">
                            <Icon margin="m-0" icon="save" />
                            <span className="ms-1">Save</span>
                        </Button>
                        <Button variant="secondary" onClick={onCancel}>
                            <Icon icon="close" />
                            Cancel
                        </Button>
                    </RichPanelActions>
                </RichPanelHeader>

                <RichPanelDetailsEditSchema schemaValidatorCollections={schemaValidatorCollections} collectionName="" />
            </RichPanel>
        </FormProvider>
    );
};

const formSchema = yup.object({
    collection: yup.string().required(),
    schema: yup
        .string()
        .required()
        .test("is-json", "Invalid JSON", (value) => !!value && jsonUtil.isValidJson(value)),
});

export type DocumentSchemaFormData = yup.InferType<typeof formSchema>;

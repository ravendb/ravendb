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
import { FormAceEditor, FormGroup, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace/lib/ace";
import useBoolean from "hooks/useBoolean";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import jsonUtil from "common/jsonUtil";
import * as yup from "yup";
import { TestContext } from "yup";
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
import DocumentSchemaFilter from "components/pages/database/settings/documentSchema/DocumentSchemaFilter";
import Ajv from "ajv";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { ValidationSchemaViewSheetPanel } from "components/pages/database/settings/documentSchema/partials/ValidationSchemaViewSheetPanel";
import { ScriptSyntaxHelp } from "components/pages/database/settings/documentSchema/partials/ScriptSyntaxHelp";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import classNames from "classnames";
import { StickyHeader } from "components/common/StickyHeader";

const ajv = new Ajv({
    allErrors: true,
    strictTypes: true,
});

export default function DocumentSchema() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasSchemaValidation = useAppSelector(licenseSelectors.statusValue("HasSchemaValidation"));

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
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading
                        marginBottom={4}
                        title="Document Schema"
                        icon="document-schema"
                        licenseBadgeText={hasSchemaValidation ? null : "Professional +"}
                    />
                    <StickyHeader>
                        <DocumentSchemaSelectActions />

                        <div className={hasSchemaValidation ? "" : "item-disabled pe-none"}>
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
                        </div>
                    </StickyHeader>
                    <div className="mt-4">
                        <HrHeader
                            count={filteredValidators.length}
                            right={
                                <ConditionalPopover
                                    conditions={{
                                        isActive: !hasSchemaValidation,
                                        message: <FeatureNotAvailableInYourLicensePopoverBody />,
                                    }}
                                >
                                    <Button
                                        size="xs"
                                        variant="info"
                                        className="rounded-pill"
                                        onClick={handleAddNew}
                                        disabled={!hasSchemaValidation || !hasDatabaseAdminAccess}
                                        title="Click to add a new schema for a collection"
                                    >
                                        <Icon icon="plus" />
                                        Add new
                                    </Button>
                                </ConditionalPopover>
                            }
                        >
                            <Icon icon="documents" />
                            <span>Collection specific document schemas</span>
                            <PopoverWithHoverWrapper message="Define and manage JSON Schemas for each collection">
                                <Icon icon="info-new" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </HrHeader>
                        <div className={classNames(!hasSchemaValidation && "item-disabled pe-none")}>
                            <DocumentSchemaBody
                                filteredValidators={filteredValidators}
                                asyncLoadValidators={asyncLoadValidators}
                            />
                        </div>
                    </div>
                </Col>
                <Col sm={12} lg={4}>
                    <DocumentSchemaAboutView />
                </Col>
            </Row>
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
    isEditing: boolean;
    onStatusToggle: (disabled: boolean) => void;
    isTogglingState: boolean;
}

export function DocumentSchemaStatus({
    validator,
    isEditing,
    onStatusToggle,
    isTogglingState,
}: DocumentSchemaStatusProps) {
    if (isEditing) {
        return null;
    }

    return (
        <Dropdown>
            <Dropdown.Toggle disabled={isTogglingState} variant={validator.Disabled ? "warning" : "success"}>
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
    const hasSchemaValidation = useAppSelector(licenseSelectors.statusValue("HasSchemaValidation"));
    const { value: isEditingSchema, toggle: toggleEditingSchema } = useBoolean(false);
    const { value: isTogglingStatus, setTrue: setTogglingStatus, setFalse: unsetTogglingStatus } = useBoolean(false);
    const { open } = useViewSheet();
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

    const isGlobalDisabled = useAppSelector(documentSchemaSelectors.isGlobalDisabled);

    const asyncSaveValidators = async (items: DocumentSchemaValidatorConfig[]) => {
        await databasesService.saveSchemaValidation(
            databaseName,
            documentSchemaUtils.mapToSchemaValidationConfigurationDto(items, isGlobalDisabled)
        );
        dispatch(documentSchemaActions.validatorsSaved());
    };

    const form = useForm<DocumentSchemaFormData>({
        resolver: yupResolver(formSchema),
        defaultValues: {
            collection: validator.Name,
            schema,
        },
        context: { schemaValidatorCollections, originalCollectionName: validator.Name },
    });

    const { handleSubmit, reset } = form;

    const handleEditSchema = async (data: DocumentSchemaFormData) => {
        const updatedItem = documentSchemaUtils.mapToDocumentSchemaValidatorConfigDto(data);
        const next = [...validatorsAll.filter((v) => v.Name !== validator.Name), updatedItem];
        await asyncSaveValidators(next);
        dispatch(documentSchemaActions.validatorEdited({ originalName: validator.Name, validator: updatedItem }));
        toggleEditingSchema();
    };

    const handleBulkStatusToggle = async (disabled: boolean) => {
        try {
            setTogglingStatus();
            const updatedValidator = { ...validator, Disabled: disabled };
            const next = [...validatorsAll.filter((v) => v.Name !== validator.Name), updatedValidator];
            await asyncSaveValidators(next);
            dispatch(
                documentSchemaActions.validatorEdited({ originalName: validator.Name, validator: updatedValidator })
            );
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

    const handleOpenSheet = () => {
        open({
            component: <ValidationSchemaViewSheetPanel validators={[validator]} />,
        });
    };

    return (
        <>
            <FormProvider {...form}>
                <RichPanel>
                    <RichPanelHeader>
                        <RichPanelInfo>
                            {(isEditingSchema || hasDatabaseAdminAccess) && (
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
                                    isEditing={isEditingSchema}
                                    onStatusToggle={handleStatusToggle}
                                    isTogglingState={isTogglingStatus}
                                />
                                <ConditionalPopover
                                    conditions={{
                                        isActive: isEditingSchema,
                                        message:
                                            "The schema must be saved in order to test it against existing documents.",
                                    }}
                                >
                                    <Button
                                        disabled={isEditingSchema}
                                        onClick={handleOpenSheet}
                                        variant="secondary"
                                        title="Click to test this schema against existing documents"
                                    >
                                        <Icon icon="rocket" margin="m-0" />
                                    </Button>
                                </ConditionalPopover>
                                <ConditionalPopover
                                    conditions={{
                                        isActive: !hasSchemaValidation,
                                        message: <FeatureNotAvailableInYourLicensePopoverBody />,
                                    }}
                                >
                                    <ButtonWithSpinner
                                        onClick={isEditingSchema ? handleSubmit(handleEditSchema) : toggleEditingSchema}
                                        variant={isEditingSchema ? "success" : "secondary"}
                                        isSpinning={form.formState.isSubmitting}
                                        disabled={!hasSchemaValidation}
                                        icon={isEditingSchema ? "save" : "edit"}
                                        title={isEditingSchema ? "Save changes to this schema" : "Edit this schema"}
                                    >
                                        {isEditingSchema ? <span className="ms-1">Save</span> : null}
                                    </ButtonWithSpinner>
                                </ConditionalPopover>
                                {isEditingSchema ? (
                                    <Button
                                        variant="secondary"
                                        disabled={form.formState.isSubmitting}
                                        onClick={() => {
                                            reset({ collection: validator.Name, schema });
                                            toggleEditingSchema();
                                        }}
                                        title="Discard changes to this schema"
                                    >
                                        <Icon icon="close" />
                                        Discard
                                    </Button>
                                ) : (
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: !hasSchemaValidation,
                                            message: <FeatureNotAvailableInYourLicensePopoverBody />,
                                        }}
                                    >
                                        <ButtonWithSpinner
                                            isSpinning={form.formState.isSubmitting}
                                            variant="danger"
                                            onClick={toggleDeleteModal}
                                            disabled={!hasSchemaValidation}
                                            icon="trash"
                                            title="Delete this schema"
                                        />
                                    </ConditionalPopover>
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
                    <FormSelectAutocomplete
                        control={control}
                        name="collection"
                        placeholder="Select a collection (or enter a new one)"
                        options={collectionOptions}
                    />
                </FormGroup>

                <FormLabel>
                    Document schema{" "}
                    <PopoverWithHoverWrapper message="Edit the JSON schema for documents in the collection">
                        <Icon icon="info" color="info" margin="m-0" />
                    </PopoverWithHoverWrapper>
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
        context: { schemaValidatorCollections },
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
    collection: yup
        .string()
        .required()
        .test(
            "is-unique-collection",
            "A schema for this collection already exists",
            function (
                value,
                ctx: TestContext<{ schemaValidatorCollections: string[]; originalCollectionName?: string }>
            ) {
                if (!value) {
                    return true;
                }
                const { schemaValidatorCollections, originalCollectionName } = ctx.options.context;

                const collectionsToCheck = originalCollectionName
                    ? schemaValidatorCollections.filter((x) => x.toLowerCase() !== originalCollectionName.toLowerCase())
                    : schemaValidatorCollections;

                return !collectionsToCheck.map((x) => x.toLowerCase()).includes(value.toLowerCase());
            }
        ),
    schema: yup
        .string()
        .required()
        .test("is-json", "Invalid JSON", (value) => !!value && jsonUtil.isValidJson(value))
        .test("is-valid-schema", "JSON does not match the JSON Schema specification", function (value) {
            if (!value || !jsonUtil.isValidJson(value)) {
                return false;
            }

            try {
                const parsed = JSON.parse(value);

                const isValid = ajv.validateSchema(parsed);
                if (!isValid) {
                    return this.createError({ message: ajv.errorsText() });
                }

                if (!parsed.properties && !parsed.type) {
                    return this.createError({ message: "Schema must define 'properties' or 'type'" });
                }

                return true;
            } catch (e) {
                return this.createError({ message: `Invalid JSON Schema: ${e.message}` });
            }
        }),
});

export type DocumentSchemaFormData = yup.InferType<typeof formSchema>;

import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import { AboutViewHeading } from "components/common/AboutView";
import { Checkbox } from "components/common/Checkbox";
import React, { useRef } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import Select, { SelectOption } from "components/common/select/Select";
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
import { LazyLoad } from "components/common/LazyLoad";
import jsonUtil from "common/jsonUtil";
import * as yup from "yup";
import { FormProvider, useForm, useFormContext } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { documentSchemaActions, DocumentSchemaValidatorConfig } from "./store/documentSchemaSlice";
import { documentSchemaSelectors } from "./store/documentSchemaSliceSelectors";
import { useServices } from "hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import DocumentSchemaSelectActions from "components/pages/database/settings/documentSchema/partials/DocumentSchemaSelectActions";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";
import DocumentSchemaAboutView from "components/pages/database/settings/documentSchema/partials/DocumentSchemaAboutView";
import DocumentSchemaDeleteModal from "components/pages/database/settings/documentSchema/partials/DocumentSchemaDeleteModal";
import { EmptySet } from "components/common/EmptySet";
import { useDocumentSchema } from "components/pages/database/settings/documentSchema/useDocumentSchema";

export default function DocumentSchema() {
    const {
        options,
        filteredValidators,
        selectedCollections,
        handleOnSelectCollection,
        asyncLoadValidators,
        handleAddNew,
    } = useDocumentSchema();

    return (
        <div className="content-margin">
            <Col>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading marginBottom={4} title="Document Schema" icon="document" />
                        <DocumentSchemaSelectActions />
                        <div className="mt-3">
                            <span className="small-label">Filter by collection</span>
                            <Select
                                isLoading={asyncLoadValidators.loading}
                                options={options}
                                isMulti
                                value={selectedCollections}
                                onChange={handleOnSelectCollection}
                                placeholder="All collections"
                                isClearable
                            />
                        </div>

                        <div className="mt-4">
                            <HrHeader
                                count={filteredValidators.length}
                                right={
                                    <Button size="xs" variant="info" className="rounded-pill" onClick={handleAddNew}>
                                        <Icon icon="plus" />
                                        Add new
                                    </Button>
                                }
                            >
                                <Icon icon="documents" />
                                <span>Collection specific document schemas</span>
                                <PopoverWithHoverWrapper message="info">
                                    <Icon icon="info" color="info" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </HrHeader>
                            <DocumentSchemaBody />
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

const DocumentSchemaBody = () => {
    const {
        handleCancelNew,
        handleNewSchemaSubmit,
        filteredValidators,
        draftIds,
        allCollectionNames,
        asyncLoadValidators,
    } = useDocumentSchema();

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
            <LazyLoad active={asyncLoadValidators.loading}>
                {filteredValidators.map((v) => (
                    <CollectionSchemaRichPanel
                        schemaValidatorCollections={allCollectionNames}
                        key={v.Name}
                        collectionName={v.Name}
                        schema={v.Schema}
                    />
                ))}
            </LazyLoad>
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
};

interface CollectionSchemaRichPanelProps {
    collectionName: string;
    schema: string;
    schemaValidatorCollections: string[];
}

const CollectionSchemaRichPanel = ({
    collectionName = "",
    schema = "",
    schemaValidatorCollections,
}: CollectionSchemaRichPanelProps) => {
    const { value: isEditingSchema, toggle: toggleEditingSchema } = useBoolean(false);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const dispatch = useAppDispatch();
    const isSelected = useAppSelector(documentSchemaSelectors.isSelectedCollectionName(collectionName));
    const validatorsAll = useAppSelector(documentSchemaSelectors.allValidators);
    const { databasesService } = useServices();
    const { value: isDeleteModalOpen, toggle: toggleDeleteModal } = useBoolean(false);

    const asyncSaveValidators = useAsyncCallback(async (items: DocumentSchemaValidatorConfig[]) => {
        await databasesService.saveSchemaValidation(
            databaseName,
            documentSchemaUtils.mapToSchemaValidationConfigurationDto(items)
        );
        dispatch(documentSchemaActions.validatorsSaved());
    });

    const form = useForm<DocumentSchemaFormData>({
        resolver: yupResolver(formSchema),
        defaultValues: {
            collection: collectionName,
            schema: jsonUtil.formatJson(schema),
        },
    });

    const { handleSubmit, reset } = form;

    const handleEditSchema = async (data: DocumentSchemaFormData) => {
        const updatedItem = documentSchemaUtils.maptoDocumentSchemaValidatorConfigDto(data);
        dispatch(documentSchemaActions.validatorEdited(updatedItem));
        const next = [...validatorsAll.filter((v) => v.Name !== updatedItem.Name), updatedItem];
        await asyncSaveValidators.execute(next);
        toggleEditingSchema();
    };

    return (
        <>
            <FormProvider {...form}>
                <RichPanel>
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelSelect>
                                <Checkbox
                                    toggleSelection={() =>
                                        dispatch(documentSchemaActions.selectedCollectionNameToggled(collectionName))
                                    }
                                    selected={isSelected}
                                />
                            </RichPanelSelect>
                            <RichPanelName>{collectionName}</RichPanelName>
                        </RichPanelInfo>
                        <RichPanelActions>
                            {/*For now schema playground is not available.*/}
                            {/*<ConditionalPopover*/}
                            {/*    conditions={{*/}
                            {/*        isActive: isEditingSchema,*/}
                            {/*        message: "The schema must be saved in order to test it against existing documents.",*/}
                            {/*    }}*/}
                            {/*>*/}
                            {/*    <Button variant="secondary" disabled={isEditingSchema}>*/}
                            {/*        <Icon margin="m-0" icon="rocket" />*/}
                            {/*    </Button>*/}
                            {/*</ConditionalPopover>*/}

                            <Button
                                onClick={isEditingSchema ? handleSubmit(handleEditSchema) : toggleEditingSchema}
                                variant={isEditingSchema ? "success" : "secondary"}
                                disabled={asyncSaveValidators.loading}
                            >
                                <Icon margin="m-0" icon={isEditingSchema ? "save" : "edit"} />
                                {isEditingSchema && <span className="ms-1">Save</span>}
                            </Button>
                            {isEditingSchema ? (
                                <Button
                                    variant="secondary"
                                    disabled={asyncSaveValidators.loading}
                                    onClick={() => {
                                        reset({ collection: collectionName, schema: jsonUtil.formatJson(schema) });
                                        toggleEditingSchema();
                                    }}
                                >
                                    <Icon icon="close" />
                                    Discard
                                </Button>
                            ) : (
                                <Button
                                    disabled={asyncSaveValidators.loading}
                                    variant="danger"
                                    onClick={toggleDeleteModal}
                                >
                                    <Icon margin="m-0" icon="trash" />
                                </Button>
                            )}
                        </RichPanelActions>
                    </RichPanelHeader>

                    {isEditingSchema && (
                        <RichPanelDetailsEditSchema
                            schemaValidatorCollections={schemaValidatorCollections}
                            collectionName={collectionName}
                        />
                    )}
                    {!isEditingSchema && <RichPanelDetailsViewSchema schema={schema} />}
                </RichPanel>
            </FormProvider>
            {isDeleteModalOpen && (
                <DocumentSchemaDeleteModal collectionName={collectionName} onHide={toggleDeleteModal} />
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
                            component: <AceEditor.HelpAction message={<div>test</div>} />,
                            position: "bottom",
                        },
                    ]}
                    mode="json"
                    value={jsonUtil.formatJson(schema) ?? ""}
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
    const { control } = useFormContext<DocumentSchemaFormData>();
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
                <FormLabel>
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
                        { component: <AceEditor.LoadFileAction onLoad={() => {}} /> },
                        {
                            component: <AceEditor.HelpAction message={<div>TODO</div>} />,
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
    schema: yup.string().required(),
});

export type DocumentSchemaFormData = yup.InferType<typeof formSchema>;

import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import { AboutViewHeading } from "components/common/AboutView";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { HrHeader } from "components/common/HrHeader";
import React, { useRef } from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import { FormAceEditor, FormGroup, FormLabel, FormSelect } from "components/common/Form";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace/lib/ace";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";
import { FormProvider, useFieldArray, UseFieldArrayRemove, useForm, useFormContext, useWatch } from "react-hook-form";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { FieldArrayWithId } from "react-hook-form/dist/types/fieldArray";
import useBoolean from "hooks/useBoolean";
import Collapse from "react-bootstrap/Collapse";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SelectOption } from "components/common/select/Select";
import { DocumentSchemaValidatorConfig } from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import DocumentSchemaPlaygroundAboutView from "components/pages/database/settings/documentSchema/partials/DocumentSchemaPlaygroundAboutView";
import { ValidationSchemaViewSheetPanel } from "components/pages/database/settings/documentSchema/partials/ValidationSchemaViewSheetPanel";
import { ScriptSyntaxHelp } from "components/pages/database/settings/documentSchema/partials/ScriptSyntaxHelp";

export default function DocumentSchemaPlayground() {
    return (
        <div className="content-margin">
            <Row className="gy-sm">
                <Col>
                    <DocumentSchemaPlaygroundBody />
                </Col>
                <Col sm={12} lg={4}>
                    {/*TODO: remove if about view is finished*/}
                    {false && <DocumentSchemaPlaygroundAboutView />}
                </Col>
            </Row>
        </div>
    );
}

function DocumentSchemaPlaygroundBody() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { appUrl } = useAppUrls();
    const { open } = useViewSheet();

    const form = useForm({
        resolver: yupResolver(schema),
        defaultValues: { documentSchemas: [{ collection: "", schema: "" }] },
    });

    const { control, handleSubmit } = form;
    const { fields, append, remove } = useFieldArray({
        control,
        name: "documentSchemas",
    });

    const handleAppendTestField = () => {
        append({ collection: "", schema: "" });
    };

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.userCollectionNames);

    const getCollectionOptions = (currentIndex: number): SelectOption[] => {
        const selectedCollections = fields.filter((_, idx) => idx !== currentIndex).map((schema) => schema.collection);

        return allCollectionNames
            .filter((collection) => !selectedCollections.includes(collection))
            .map((x) => ({
                label: x,
                value: x,
            }));
    };

    const handleOpenSheet = (formData: DocumentSchemaFormData) => {
        const validators: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">[] = formData.documentSchemas.map(
            (ds) => ({
                Name: ds.collection,
                Schema: ds.schema,
            })
        );
        open({
            component: <ValidationSchemaViewSheetPanel isPlayground validators={validators} />,
            initialWidth: "40%",
        });
    };

    return (
        <form onSubmit={handleSubmit(handleOpenSheet)}>
            <AboutViewHeading marginBottom={4} title="Document Schema Playground" icon="rocket" />
            <span>
                Quickly create and test schemas against your documents without affecting your saved data. The Schema
                Playground is a temporary workspace designed for safe experimentation.
            </span>

            <div className="mt-5 d-flex align-items-center justify-content-between">
                <a href={appUrl.forDocumentSchema(databaseName)} className="btn btn-secondary">
                    <Icon icon="close" />
                    Cancel
                </a>
                <Button type="submit" className="rounded-pill">
                    <Icon icon="rocket" />
                    Run test
                </Button>
            </div>

            <div className="mt-4">
                <HrHeader
                    count={fields.length}
                    right={
                        <Button onClick={handleAppendTestField} size="xs" variant="info" className="rounded-pill">
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

                <FormProvider {...form}>
                    {fields.map((field, index) => (
                        <TestDocumentSchema
                            collectionOptions={getCollectionOptions(index)}
                            remove={remove}
                            key={field.id}
                            {...field}
                            index={index}
                        />
                    ))}
                </FormProvider>
            </div>
        </form>
    );
}

type TestDocumentSchemaProps = FieldArrayWithId<DocumentSchemaFormData, "documentSchemas", "id"> & {
    index: number;
    remove: UseFieldArrayRemove;
    collectionOptions: SelectOption[];
};

function TestDocumentSchema({ index, remove, collectionOptions }: TestDocumentSchemaProps) {
    const { value: isPanelCollapsed, toggle: togglePanelCollapse } = useBoolean(false);
    const { control, setValue } = useFormContext<DocumentSchemaFormData>();
    const aceRef = useRef<ReactAce>(null);

    const field = useWatch({
        control,
        name: `documentSchemas.${index}`,
    });

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>{field?.collection || `Document schema ${index + 1}`}</RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    <Button onClick={() => remove(index)} variant="danger">
                        <Icon margin="m-0" icon="trash" />
                    </Button>
                    <Button onClick={togglePanelCollapse} variant="secondary">
                        <Icon margin="m-0" icon={isPanelCollapsed ? "expand-vertical" : "collapse-vertical"} />
                    </Button>
                </RichPanelActions>
            </RichPanelHeader>

            <Collapse in={!isPanelCollapsed} mountOnEnter unmountOnExit>
                <div>
                    <RichPanelDetails>
                        <FormGroup className="w-100 mt-2">
                            <FormGroup>
                                <FormLabel>Collection</FormLabel>
                                <FormSelect
                                    control={control}
                                    name={`documentSchemas.${index}.collection`}
                                    placeholder="Select a collection (or enter a new one)"
                                    options={collectionOptions}
                                />
                            </FormGroup>
                            <FormLabel>
                                Document schema <Icon icon="info" color="info" margin="m-0" />
                            </FormLabel>
                            <FormAceEditor
                                control={control}
                                name={`documentSchemas.${index}.schema`}
                                height="500px"
                                aceRef={aceRef}
                                isFullScreenLabelHidden
                                actions={[
                                    { component: <AceEditor.FullScreenAction /> },
                                    { component: <AceEditor.FormatAction /> },
                                    {
                                        component: (
                                            <AceEditor.LoadFileAction
                                                onLoad={(value) => {
                                                    setValue(`documentSchemas.${index}.schema`, value, {
                                                        shouldValidate: true,
                                                    });
                                                }}
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
                </div>
            </Collapse>
        </RichPanel>
    );
}

const schema = yup.object({
    documentSchemas: yup.array().of(
        yup.object({
            collection: yup.string().required(),
            schema: yup.string().required(),
        })
    ),
});

type DocumentSchemaFormData = yup.InferType<typeof schema>;

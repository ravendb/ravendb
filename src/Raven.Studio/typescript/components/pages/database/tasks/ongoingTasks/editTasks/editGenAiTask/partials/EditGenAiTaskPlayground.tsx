import { useFieldArray, useWatch } from "react-hook-form";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { useServices } from "components/hooks/useServices";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import documentMetadata from "models/database/documents/documentMetadata";
import {
    FormAceEditor,
    FormGroup,
    FormLabel,
    FormSelectAutocomplete,
    FormSwitch,
    FormValidationMessage,
} from "components/common/Form";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import Collapse from "react-bootstrap/Collapse";
import useConfirm from "components/common/ConfirmDialog";
import RichAlert from "components/common/RichAlert";
import classNames from "classnames";
import { useEffect, useRef, useState } from "react";
import { Switch } from "components/common/Checkbox";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import EditGenAiTaskFormVirtualList from "./EditGenAiTaskFormVirtualList";
import { SelectOption } from "components/common/select/Select";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";

type PlaygroundTab = "document" | "context" | "modelOutput";

export default function EditGenAiTaskPlayground() {
    const dispatch = useAppDispatch();

    const documentRef = useRef<ReactAce>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);
    const isPlaygroundCollapsed = useAppSelector(editGenAiTaskSelectors.isPlaygroundCollapsed);
    const isPlaygroundEditMode = useAppSelector(editGenAiTaskSelectors.isPlaygroundEditMode);
    const isDocumentInfoVisible = useAppSelector(editGenAiTaskSelectors.isDocumentInfoVisible);
    const isContextInfoVisible = useAppSelector(editGenAiTaskSelectors.isContextInfoVisible);
    const isModelInputInfoVisible = useAppSelector(editGenAiTaskSelectors.isModelInputInfoVisible);

    const {
        control,
        setValue,
        formState: { errors },
        clearErrors,
    } = useFormContext<EditGenAiTaskFormData>();

    const confirm = useConfirm();

    const formValues = useWatch({ control });

    const contextsFieldsArray = useFieldArray({
        control,
        name: "playgroundContexts",
    });

    const modelOutputsFieldsArray = useFieldArray({
        control,
        name: "playgroundModelOutputs",
    });

    const { databasesService } = useServices();

    const asyncGetDocumentIdOptions = useAsyncDebounce(
        async () => {
            const result = await databasesService.getDocumentsMetadataByIDPrefix(
                formValues.documentId,
                10,
                databaseName
            );
            return result.map((x) => x["@metadata"]["@id"]).map((x) => ({ value: x, label: x }));
        },
        [formValues.documentId],
        300
    );

    const handleDocumentIdChange = async ({ value: documentId }: SelectOption) => {
        setValue("documentId", documentId);

        const result = await databasesService.getDocumentWithMetadata(documentId, databaseName);
        const docDto = result.toDto(true);
        const metaDto = docDto["@metadata"];
        documentMetadata.filterMetadata(metaDto);
        setValue("playgroundDocument", JSON.stringify(docDto, null, 4));
    };

    const handleEditModeToggle = async (isSelected: boolean): Promise<boolean> => {
        if (isSelected) {
            const isConfirmed = await confirm({
                title: "You’re about to enter Playground edit mode",
                message: (
                    <>
                        While in Playground edit mode, you can modify the selected content as you wish.
                        <br />
                        Be aware that any changes made to the content in this mode will NOT be saved to the original
                        element.
                    </>
                ),
                actionColor: "warning",
                confirmIcon: "arrow-right",
                confirmText: "Enter edit mode",
                icon: "edited",
                size: "lg",
            });

            if (isConfirmed) {
                dispatch(editGenAiTaskActions.isPlaygroundEditModeToggled());
                return true;
            } else {
                return false;
            }
        } else {
            dispatch(editGenAiTaskActions.isPlaygroundEditModeToggled());
            return true;
        }
    };

    const handleProvideContentManually = async () => {
        if (await handleEditModeToggle(!isPlaygroundEditMode)) {
            setValue("playgroundDocument", "{}", { shouldValidate: true });
        }
    };

    const [activeTab, setActiveTab] = useState<PlaygroundTab>("document");

    useEffect(() => {
        if (currentStep === "modelInput") {
            return setActiveTab("context");
        }

        if (currentStep === "updateScript") {
            return setActiveTab("modelOutput");
        }

        setActiveTab("document");
    }, [currentStep]);

    useEffect(() => {
        if (formValues.playgroundDocument) {
            clearErrors("playgroundDocument");
        }
    }, [formValues.playgroundDocument]);

    // Set initial document ID based on collection name
    useEffect(() => {
        if (formValues.collectionName) {
            setValue("documentId", formValues.collectionName + "/");
        }
    }, [formValues.collectionName]);

    return (
        <div className="playground">
            <div className="hstack">
                <div>
                    Playground
                    <PopoverWithHoverWrapper message="Use the playground to select/enter sample content for testing the outcome of this configuration step.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </div>
                <div className="playground-line"></div>
                <div>
                    <Button
                        variant="link"
                        size="xs"
                        onClick={() => dispatch(editGenAiTaskActions.isPlaygroundCollapsedToggled())}
                    >
                        <Icon icon={isPlaygroundCollapsed ? "expand-vertical" : "collapse-vertical"} />
                        {isPlaygroundCollapsed ? "Expand" : "Collapse"}
                    </Button>
                </div>
            </div>
            <Collapse in={!isPlaygroundCollapsed} mountOnEnter unmountOnExit>
                <div className="panel-bg-1 border border-secondary rounded-2 mt-1">
                    <Tab.Container id="playground-tabs" activeKey={activeTab}>
                        <div className="hstack panel-bg-2 border-bottom border-secondary p-2 justify-content-between border-top-left-radius border-top-right-radius">
                            <Nav className="gap-2">
                                <Nav.Item onClick={() => setActiveTab("document")}>
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: currentStep !== "context" && currentStep !== "updateScript",
                                            message: (
                                                <>
                                                    The selected document has no effect on testing in this step.
                                                    <br />
                                                    <br />
                                                    The test uses the context objects (generated in the previous
                                                    playground step or customized here), along with the prompt and JSON
                                                    schema.
                                                </>
                                            ),
                                        }}
                                    >
                                        <Nav.Link
                                            eventKey="document"
                                            className={classNames("no-decor", {
                                                "opacity-50":
                                                    currentStep !== "context" && currentStep !== "updateScript",
                                            })}
                                        >
                                            <Icon icon="document" />
                                            Document
                                        </Nav.Link>
                                    </ConditionalPopover>
                                </Nav.Item>
                                {(currentStep === "modelInput" || currentStep === "updateScript") && (
                                    <Nav.Item onClick={() => setActiveTab("context")}>
                                        <Nav.Link eventKey="context" className="no-decor">
                                            <Icon icon="indent" />
                                            Context input
                                        </Nav.Link>
                                    </Nav.Item>
                                )}
                                {currentStep === "updateScript" && (
                                    <Nav.Item onClick={() => setActiveTab("modelOutput")}>
                                        <ConditionalPopover
                                            conditions={{
                                                isActive: currentStep !== "updateScript",
                                                message:
                                                    "This configuration doesn’t give any additional context to the active step.",
                                            }}
                                        >
                                            <Nav.Link
                                                eventKey="modelOutput"
                                                className={classNames("no-decor", {
                                                    "text-muted": currentStep !== "updateScript",
                                                })}
                                            >
                                                <Icon icon="resources" />
                                                Model output
                                            </Nav.Link>
                                        </ConditionalPopover>
                                    </Nav.Item>
                                )}
                            </Nav>
                            <div>
                                <Switch
                                    id="editMode"
                                    toggleSelection={(e) => handleEditModeToggle(e.target.checked)}
                                    selected={isPlaygroundEditMode}
                                    color="info"
                                    className="mb-0"
                                >
                                    Edit mode
                                    <PopoverWithHoverWrapper
                                        message={
                                            <>
                                                When in &quot;Edit mode&quot;, you can modify the content in the
                                                Playground as you wish.
                                                <br />
                                                Be aware that any changes made to the content in this mode will NOT be
                                                saved to the original element.
                                            </>
                                        }
                                    >
                                        <Icon icon="info" color="info" margin="ms-1" />
                                    </PopoverWithHoverWrapper>
                                </Switch>
                            </div>
                        </div>

                        <Tab.Content className="p-3">
                            <Tab.Pane eventKey="document">
                                {!formValues.playgroundDocument && (
                                    <div className="vstack align-items-center py-3">
                                        {isDocumentInfoVisible && (
                                            <RichAlert
                                                variant="info"
                                                className="mb-3"
                                                onCancel={() =>
                                                    dispatch(editGenAiTaskActions.isDocumentInfoVisibleSet(false))
                                                }
                                            >
                                                In this playground area, you can select a document in order to test the
                                                outcome of the context generation script and view the resulting context
                                                object(s).
                                                <br />
                                                Choose an existing document or manually enter a new one (Edit mode).
                                            </RichAlert>
                                        )}
                                        {errors.playgroundDocument && (
                                            <FormValidationMessage className="d-flex justify-content-center mt-2">
                                                {errors.playgroundDocument.message}
                                            </FormValidationMessage>
                                        )}
                                        <FormGroup>
                                            <FormLabel>Select a document from the source collection</FormLabel>
                                            <FormSelectAutocomplete
                                                control={control}
                                                name="documentId"
                                                placeholder="E.g. Posts/01"
                                                options={asyncGetDocumentIdOptions.result ?? []}
                                                isLoading={asyncGetDocumentIdOptions.loading}
                                                onChange={handleDocumentIdChange}
                                            />
                                        </FormGroup>
                                        <Button variant="link" onClick={handleProvideContentManually} size="sm">
                                            <Icon icon="edit" />
                                            Or enter a document manually
                                        </Button>
                                    </div>
                                )}
                                {formValues.playgroundDocument && (
                                    <div>
                                        <div
                                            className={classNames("hstack mb-1", {
                                                "justify-content-end": !formValues.documentId,
                                                "justify-content-between": formValues.documentId,
                                            })}
                                        >
                                            {formValues.documentId && (
                                                <div>
                                                    Selected document: <strong>{formValues.documentId}</strong>
                                                </div>
                                            )}
                                            <Button
                                                variant="link"
                                                size="sm"
                                                onClick={() => setValue("playgroundDocument", "")}
                                            >
                                                <Icon icon="reset" />
                                                Reset selection
                                            </Button>
                                        </div>
                                        <FormAceEditor
                                            aceRef={documentRef}
                                            control={control}
                                            name="playgroundDocument"
                                            mode="json"
                                            readOnly={!isPlaygroundEditMode}
                                            actions={[
                                                { component: <AceEditor.FullScreenAction /> },
                                                { component: <AceEditor.FormatAction /> },
                                                isPlaygroundEditMode
                                                    ? {
                                                          component: (
                                                              <AceEditor.LoadFileAction
                                                                  onLoad={(value) =>
                                                                      setValue("playgroundDocument", value, {
                                                                          shouldValidate: true,
                                                                      })
                                                                  }
                                                              />
                                                          ),
                                                      }
                                                    : null,
                                            ]}
                                        />
                                    </div>
                                )}
                            </Tab.Pane>
                            <Tab.Pane eventKey="context">
                                {isContextInfoVisible && (
                                    <RichAlert
                                        variant="info"
                                        className="mb-3"
                                        onCancel={() => dispatch(editGenAiTaskActions.isContextInfoVisibleSet(false))}
                                    >
                                        This playground area shows the context objects generated in the previous step.
                                        <br />
                                        Alternatively, you can enter custom context objects manually using Edit mode.
                                        <br />
                                        You can then test the model’s response based on the combined input: the context
                                        objects, and the prompt and schema defined in this step.
                                    </RichAlert>
                                )}
                                <FormGroup className="hstack justify-content-end" marginClass="mb-2">
                                    <FormSwitch control={control} name="isForceSendingCachedObjects">
                                        Force reprocess
                                        <PopoverWithHoverWrapper
                                            message={
                                                <>
                                                    When enabled, clicking &quot;Test model&quot; will send a test
                                                    request to the model even if the sample document and context objects
                                                    (from the playground), and the prompt and JSON schema (from the task
                                                    definition) haven’t changed.
                                                </>
                                            }
                                        >
                                            <Icon icon="info" color="info" margin="ms-1" />
                                        </PopoverWithHoverWrapper>
                                    </FormSwitch>
                                </FormGroup>
                                {isPlaygroundEditMode && (
                                    <Button
                                        variant="primary"
                                        onClick={() =>
                                            contextsFieldsArray.prepend({
                                                value: "{}",
                                                idx: null,
                                                aiHash: null,
                                                isCached: false,
                                            })
                                        }
                                        className="mb-2"
                                    >
                                        <Icon icon="plus" />
                                        Add new context object
                                    </Button>
                                )}
                                <div
                                    style={{ height: getVirtualListHeight(contextsFieldsArray.fields.length) }}
                                    className="d-flex"
                                >
                                    <EditGenAiTaskFormVirtualList
                                        fields={contextsFieldsArray.fields}
                                        name="playgroundContexts"
                                        isReadOnly={!isPlaygroundEditMode}
                                        handleRemove={contextsFieldsArray.remove}
                                    />
                                </div>
                            </Tab.Pane>
                            <Tab.Pane eventKey="modelOutput">
                                {isModelInputInfoVisible && (
                                    <RichAlert
                                        variant="info"
                                        className="mb-3"
                                        onCancel={() =>
                                            dispatch(editGenAiTaskActions.isModelInputInfoVisibleSet(false))
                                        }
                                    >
                                        This playground area shows the model output objects generated in the previous
                                        step.
                                        <br />
                                        Alternatively, you can enter custom output objects manually using Edit mode.
                                        <br />
                                        You can then test to see how the &quot;update script&quot; would affect the
                                        document that was selected in the first playground step.
                                    </RichAlert>
                                )}
                                {isPlaygroundEditMode && (
                                    <Button
                                        variant="primary"
                                        onClick={() =>
                                            modelOutputsFieldsArray.prepend({
                                                value: "{}",
                                                idx: null,
                                            })
                                        }
                                        className="mb-2"
                                    >
                                        <Icon icon="plus" />
                                        Add new output object
                                    </Button>
                                )}
                                <div
                                    style={{ height: getVirtualListHeight(modelOutputsFieldsArray.fields.length) }}
                                    className="d-flex"
                                >
                                    <EditGenAiTaskFormVirtualList
                                        fields={modelOutputsFieldsArray.fields}
                                        name="playgroundModelOutputs"
                                        isReadOnly={!isPlaygroundEditMode}
                                        handleRemove={modelOutputsFieldsArray.remove}
                                    />
                                </div>
                            </Tab.Pane>
                        </Tab.Content>
                    </Tab.Container>
                </div>
            </Collapse>
        </div>
    );
}

function getVirtualListHeight(count: number): `${number}px` {
    if (count <= 1) {
        return "200px";
    }

    if (count === 2) {
        return "450px";
    }

    return "500px";
}

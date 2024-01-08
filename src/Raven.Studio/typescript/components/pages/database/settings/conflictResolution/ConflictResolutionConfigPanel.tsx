import React from "react";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import { Button, Collapse, Form, InputGroup, Label, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import { EditConflictResolutionSyntaxModal } from "components/pages/database/settings/conflictResolution/EditConflictResolutionSyntaxModal";
import { useAppDispatch, useAppSelector } from "components/store";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import useBoolean from "hooks/useBoolean";
import useId from "hooks/useId";
import genUtils from "common/generalUtils";
import {
    ConflictResolutionCollectionConfig,
    conflictResolutionActions,
    conflictResolutionSelectors,
} from "./store/conflictResolutionSlice";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import * as yup from "yup";
import { FormAceEditor, FormSelectCreatable } from "components/common/Form";
import { yupResolver } from "@hookform/resolvers/yup";

interface ConflictResolutionConfigPanelProps {
    isDatabaseAdmin: boolean;
    initialConfig: ConflictResolutionCollectionConfig;
}

export default function ConflictResolutionConfigPanel({
    isDatabaseAdmin,
    initialConfig,
}: ConflictResolutionConfigPanelProps) {
    const dispatch = useAppDispatch();
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );
    const usedCollectionNames = useAppSelector(conflictResolutionSelectors.usedCollectionNames);
    const collectionOptions = allCollectionNames
        .filter((x) => !usedCollectionNames.includes(x))
        .map((x) => ({ label: x, value: x }));

    const { control, handleSubmit } = useForm<FormData>({
        defaultValues: {
            collectionName: initialConfig.name,
            script: initialConfig.script,
        },
        resolver: yupResolver(getSchema(initialConfig.name, usedCollectionNames)),
    });

    const formValues = useWatch({ control });

    const { value: isSyntaxModalOpen, toggle: toggleIsSyntaxModalOpen } = useBoolean(false);

    const scriptPanelId = useId("scriptPanel");
    const unsavedChangesId = useId("unsavedChanges");
    const configId = initialConfig.id;

    const save: SubmitHandler<FormData> = (formData) => {
        dispatch(
            conflictResolutionActions.saveEdit({
                id: configId,
                newConfig: {
                    name: formData.collectionName,
                    script: formData.script,
                },
            })
        );
    };

    return (
        <RichPanel className="flex-row" id={scriptPanelId}>
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>
                            {formValues.collectionName || (
                                <>
                                    Collection name
                                    <span id={unsavedChangesId} className="text-warning">
                                        *
                                    </span>
                                    <UncontrolledTooltip target={unsavedChangesId}>
                                        The script has not been saved yet
                                    </UncontrolledTooltip>
                                </>
                            )}
                        </RichPanelName>
                    </RichPanelInfo>
                    <Form onSubmit={handleSubmit(save)}>
                        <RichPanelActions>
                            <PanelActions
                                isDatabaseAdmin={isDatabaseAdmin}
                                isInEditMode={initialConfig.isInEditMode}
                                configId={configId}
                            />
                        </RichPanelActions>
                    </Form>
                </RichPanelHeader>
                <Collapse isOpen={!initialConfig.isInEditMode}>
                    <RichPanelDetails>
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="clock" />
                                    Last modified
                                </>
                            }
                        >
                            {genUtils.formatUtcDateAsLocal(initialConfig.lastModifiedTime)}
                        </RichPanelDetailItem>
                    </RichPanelDetails>
                </Collapse>
                <Collapse isOpen={initialConfig.isInEditMode}>
                    <RichPanelDetails className="vstack gap-3 p-3">
                        {!initialConfig.name && (
                            <InputGroup className="vstack mb-1">
                                <Label>Collection</Label>
                                <FormSelectCreatable
                                    control={control}
                                    name="collectionName"
                                    placeholder="Select collection (or enter a new one)"
                                    options={collectionOptions}
                                    isClearable={false}
                                    maxMenuHeight={300}
                                    isDisabled={!isDatabaseAdmin}
                                />
                            </InputGroup>
                        )}
                        <InputGroup className="vstack">
                            <Label className="d-flex flex-wrap justify-content-between">
                                Script
                                <Button
                                    color="link"
                                    size="xs"
                                    onClick={toggleIsSyntaxModalOpen}
                                    className="p-0 align-self-end"
                                >
                                    Syntax
                                    <Icon icon="help" margin="ms-1" />
                                </Button>
                            </Label>
                            {isSyntaxModalOpen && (
                                <EditConflictResolutionSyntaxModal toggle={toggleIsSyntaxModalOpen} />
                            )}
                            <FormAceEditor
                                control={control}
                                name="script"
                                mode="javascript"
                                height="400px"
                                readOnly={!isDatabaseAdmin}
                            />
                        </InputGroup>
                    </RichPanelDetails>
                </Collapse>
            </div>
        </RichPanel>
    );
}

function PanelActions({
    isDatabaseAdmin,
    isInEditMode,
    configId,
}: {
    isDatabaseAdmin: boolean;
    isInEditMode: boolean;
    configId: string;
}) {
    const dispatch = useAppDispatch();

    if (isDatabaseAdmin) {
        if (isInEditMode) {
            return (
                <React.Fragment key="actions-in-edit">
                    <Button type="submit" color="success" title="Save changes">
                        <Icon icon="save" margin="m-0" /> Save
                    </Button>
                    <Button
                        type="button"
                        color="secondary"
                        title="Discard changes"
                        onClick={() => dispatch(conflictResolutionActions.discardEdit(configId))}
                    >
                        <Icon icon="cancel" margin="m-0" /> Discard
                    </Button>
                </React.Fragment>
            );
        } else {
            return (
                <React.Fragment key="actions-not-in-edit">
                    <Button
                        type="button"
                        color="secondary"
                        title="Edit this script"
                        onClick={() => dispatch(conflictResolutionActions.edit(configId))}
                    >
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                    <Button
                        type="button"
                        color="danger"
                        title="Delete this script"
                        onClick={() => dispatch(conflictResolutionActions.delete(configId))}
                    >
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </React.Fragment>
            );
        }
    }

    if (isInEditMode) {
        return (
            <Button
                type="button"
                color="secondary"
                title="Hide this script"
                onClick={() => dispatch(conflictResolutionActions.discardEdit(configId))}
            >
                <Icon icon="preview-off" margin="m-0" />
            </Button>
        );
    } else {
        return (
            <Button
                type="button"
                color="secondary"
                title="Show this script"
                onClick={() => dispatch(conflictResolutionActions.edit(configId))}
            >
                <Icon icon="preview" margin="m-0" />
            </Button>
        );
    }
}

function getSchema(initialName: string, usedCollectionNames: string[]) {
    return yup.object({
        collectionName: yup
            .string()
            .required()
            .notOneOf(
                usedCollectionNames.filter((x) => x !== initialName),
                "This collection name is already used"
            ),
        script: yup.string().required(),
    });
}

type FormData = yup.InferType<ReturnType<typeof getSchema>>;

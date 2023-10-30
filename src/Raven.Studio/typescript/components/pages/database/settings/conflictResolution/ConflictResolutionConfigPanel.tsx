import React, { useState } from "react";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
} from "components/common/RichPanel";
import { Button, Collapse, InputGroup, Label, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FormAceEditor, FormSelectCreatable } from "components/common/Form";
import { EditConflictResolutionSyntaxModal } from "components/pages/database/settings/conflictResolution/EditConflictResolutionSyntaxModal";
import { useAppSelector } from "components/store";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SelectOption } from "components/common/select/Select";
import { useForm } from "react-hook-form";
import useBoolean from "hooks/useBoolean";
import useId from "hooks/useId";
import useConfirm from "components/common/ConfirmDialog";

interface ConflictResolutionConfigPanelProps {
    isDatabaseAdmin: boolean;
}

export default function ConflictResolutionConfigPanel(props: ConflictResolutionConfigPanelProps) {
    const { isDatabaseAdmin } = props;

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );

    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);

    const [customCollectionOptions] = useState<SelectOption[]>([]);

    const { control } = useForm<null>({});

    const [isSyntaxModalOpen, setSyntaxModalOpen] = useState(false);

    const toggleSyntaxModalOpen = () => {
        setSyntaxModalOpen(!isSyntaxModalOpen);
    };

    const scriptPanelId = useId("scriptPanel");
    const unsavedChangesId = useId("unsavedChanges");

    return (
        <RichPanel className="flex-row" id={scriptPanelId}>
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>
                            Collection name
                            <span id={unsavedChangesId} className="text-warning">
                                *
                            </span>
                            <UncontrolledTooltip target={unsavedChangesId}>
                                The script has not been saved yet
                            </UncontrolledTooltip>
                        </RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        {isDatabaseAdmin && (
                            <>
                                <Button color="secondary" title="Edit this script" onClick={togglePanelCollapsed}>
                                    <Icon icon="edit" margin="m-0" />
                                </Button>
                                <Button color="danger" title="Delete this script">
                                    <Icon icon="trash" margin="m-0" />
                                </Button>
                            </>
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                <Collapse isOpen={!panelCollapsed}>
                    <RichPanelDetails className="vstack gap-3 p-3">
                        <InputGroup className="vstack mb-1">
                            <Label>Collection</Label>
                            <FormSelectCreatable
                                control={control}
                                name="Collections"
                                options={allCollectionNames.map((x) => ({ label: x, value: x }))}
                                customOptions={customCollectionOptions}
                                controlShouldRenderValue={false}
                                isClearable={false}
                                placeholder="Select collection (or enter a new one)"
                                maxMenuHeight={300}
                            />
                        </InputGroup>
                        <InputGroup className="vstack">
                            <Label className="d-flex flex-wrap justify-content-between">
                                Script
                                <Button
                                    color="link"
                                    size="xs"
                                    onClick={toggleSyntaxModalOpen}
                                    className="p-0 align-self-end"
                                >
                                    Syntax
                                    <Icon icon="help" margin="ms-1" />
                                </Button>
                            </Label>
                            {isSyntaxModalOpen && (
                                <EditConflictResolutionSyntaxModal
                                    isOpen={isSyntaxModalOpen}
                                    toggle={toggleSyntaxModalOpen}
                                />
                            )}
                            <FormAceEditor
                                name="conflictResolutionScript"
                                control={control}
                                mode={"javascript"}
                                height="400px"
                            />
                        </InputGroup>
                    </RichPanelDetails>
                </Collapse>
            </div>
        </RichPanel>
    );
}

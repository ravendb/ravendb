import React, { useState } from "react";
import { Button, Col, Input, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelSelect,
    RichPanelStatus,
} from "components/common/RichPanel";
import { HrHeader } from "components/common/HrHeader";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import EditRevision from "components/pages/database/settings/documentRevisions/EditRevision";
import EnforceConfiguration from "components/pages/database/settings/documentRevisions/EnforceConfiguration";
import { todo } from "common/developmentHelper";

interface DocumentRevisionsProps {
    toggle: () => void;
    selected: boolean;
    toggleSelection: () => void;
}

export default function DocumentRevisions(props: DocumentRevisionsProps) {
    const { selected, toggleSelection } = props;

    const [isEditRevisionModalOpen, setIsEditRevisionModalOpen] = useState(false);
    const toggleEditRevisionModal = () => setIsEditRevisionModalOpen(!isEditRevisionModalOpen);

    const [isNewRevisionModalOpen, setIsNewRevisionModalOpen] = useState(false);
    const toggleNewRevisionModal = () => setIsNewRevisionModalOpen(!isNewRevisionModalOpen);

    const [isEnforceConfigurationModalOpen, setIsEnforceConfigurationModalOpen] = useState(false);
    const toggleEnforceConfigurationModal = () => setIsEnforceConfigurationModalOpen(!isEnforceConfigurationModalOpen);
    const handleConfirm = async () => {
        // Logic you want to execute when the user confirms the action
    };

    todo("Feature", "Damian", "Add logic");
    todo("Feature", "Damian", "Configure forms");
    todo("Feature", "ANY", "Connect SelectionActions component");
    todo("Feature", "ANY", "Component for limit revisions by age inputs (dd/hh/mm/ss)");
    todo("Feature", "Matteo", "Add the Revert revisions view");
    todo("Feature", "Damian", "Connect to studio");
    todo("Other", "Danielle", "Text for About this view");
    todo("Other", "ANY", "Test the view");

    return (
        <div className="content-margin">
            <EditRevision
                isOpen={isEditRevisionModalOpen}
                toggle={toggleEditRevisionModal}
                onConfirm={handleConfirm}
                taskType="edit"
                configType="defaultDocument"
            />
            <EditRevision
                isOpen={isNewRevisionModalOpen}
                toggle={toggleNewRevisionModal}
                onConfirm={handleConfirm}
                taskType="new"
                configType="collectionSpecific"
            />
            <EnforceConfiguration
                isOpen={isEnforceConfigurationModalOpen}
                toggle={toggleEnforceConfigurationModal}
                onConfirm={handleConfirm}
            />
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Document Revisions" icon="revisions" />
                        <div className="d-flex gap-2">
                            <Button color="primary">
                                <Icon icon="save" />
                                Save
                            </Button>
                            <FlexGrow />
                            <Button color="secondary">
                                <Icon icon="revert-revisions" />
                                Revert revisions
                            </Button>
                            <Button color="secondary" onClick={toggleEnforceConfigurationModal}>
                                <Icon icon="rocket" />
                                Enforce configuration
                            </Button>
                        </div>
                        <div className="mt-5">
                            <Checkbox
                                selected={selected}
                                indeterminate={null}
                                toggleSelection={toggleSelection}
                                color="primary"
                                title="Select all or none"
                                size="lg"
                            >
                                <span className="small-label">Select All</span>
                            </Checkbox>
                        </div>
                        <div className="mt-5">
                            <HrHeader
                                right={
                                    <Button
                                        color="info"
                                        size="sm"
                                        className="rounded-pill"
                                        onClick={toggleEditRevisionModal}
                                    >
                                        Add new
                                    </Button>
                                }
                            >
                                <Icon icon="default" />
                                Defaults
                            </HrHeader>
                            <RichPanel className="flex-row with-status">
                                <RichPanelStatus color="success">Enabled</RichPanelStatus>
                                <div className="flex-grow-1">
                                    <RichPanelHeader>
                                        <RichPanelInfo>
                                            <RichPanelSelect>
                                                <Input type="checkbox" checked={selected} onChange={toggleSelection} />
                                            </RichPanelSelect>
                                            <RichPanelName>Default Name</RichPanelName>
                                        </RichPanelInfo>
                                        <RichPanelActions>
                                            <Button color="secondary">
                                                <Icon icon="disable" />
                                                Disable
                                            </Button>
                                            <Button color="secondary" onClick={toggleEditRevisionModal}>
                                                <Icon icon="edit" margin="m-0" />
                                            </Button>
                                            <Button color="danger">
                                                <Icon icon="trash" margin="m-0" />
                                            </Button>
                                        </RichPanelActions>
                                    </RichPanelHeader>
                                    <RichPanelDetails>
                                        <RichPanelDetailItem>
                                            <Icon icon="empty-set" />
                                            Purge revisions on document delete
                                        </RichPanelDetailItem>
                                        <RichPanelDetailItem
                                            label={
                                                <>
                                                    <Icon icon="documents" />
                                                    Keep
                                                </>
                                            }
                                        >
                                            Num of revisions
                                        </RichPanelDetailItem>
                                        <RichPanelDetailItem
                                            label={
                                                <>
                                                    <Icon icon="clock" />
                                                    Retention time
                                                </>
                                            }
                                        >
                                            Time count
                                        </RichPanelDetailItem>
                                        <RichPanelDetailItem
                                            label={
                                                <>
                                                    <Icon icon="trash" />
                                                    Delete on update
                                                </>
                                            }
                                        >
                                            Num of revisions
                                        </RichPanelDetailItem>
                                    </RichPanelDetails>
                                </div>
                            </RichPanel>
                        </div>
                        <div className="mt-5">
                            <HrHeader
                                right={
                                    <Button
                                        color="info"
                                        size="sm"
                                        className="rounded-pill"
                                        onClick={toggleNewRevisionModal}
                                    >
                                        Add new
                                    </Button>
                                }
                            >
                                <Icon icon="documents" />
                                Collections
                            </HrHeader>
                            <EmptySet>No collection specific configuration has been defined</EmptySet>
                        </div>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                targetId="1"
                                icon="about"
                                color="info"
                                description="Get additional info on what this feature can offer you"
                                heading="About this view"
                            >
                                <p>
                                    <strong>Document Revisions</strong> is a feature that allows developers to keep
                                    track of changes made to a document over time. When a document is updated in
                                    RavenDB, a new revision is automatically created, preserving the previous version of
                                    the document. This is particularly useful for scenarios where historical data and
                                    versioning are crucial.
                                </p>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="https://ravendb.net/l/SOMRWC/6.0/Csharp" target="_blank">
                                    <Icon icon="newtab" /> Docs - Document Revisions
                                </a>
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

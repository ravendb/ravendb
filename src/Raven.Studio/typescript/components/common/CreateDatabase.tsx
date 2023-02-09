import React, { useState } from "react";
import {
    Button,
    CloseButton,
    FormGroup,
    Input,
    Label,
    Modal,
    ModalBody,
    ModalFooter,
    UncontrolledPopover,
} from "reactstrap";
import { FlexGrow } from "./FlexGrow";
import { Icon } from "./Icon";
import { PropSummary, PropSummaryItem, PropSummaryName, PropSummaryValue } from "./PropSummary";
import { Steps } from "./Steps";

interface CreateDatabaseProps {
    createDatabaseModal: boolean;
    toggleCreateDatabase: () => void;
}

export function CreateDatabase(props: CreateDatabaseProps) {
    const { createDatabaseModal, toggleCreateDatabase } = props;
    const [currentStep, setCurrentStep] = useState(0);
    const stepsList = ["Setup", "Encryption", "Replication & Sharding", "Manual Node Selection", "Paths Configuration"];

    const lastStep = stepsList.length - 2 < currentStep ? true : false;
    const firstStep = currentStep < 1 ? true : false;

    const nextStep = () => {
        if (!lastStep) setCurrentStep(currentStep + 1);
    };

    const prevStep = () => {
        if (!firstStep) setCurrentStep(currentStep - 1);
    };

    return (
        <Modal
            isOpen={createDatabaseModal}
            toggle={toggleCreateDatabase}
            size="lg"
            centered
            autoFocus
            fade
            container="#OverlayContainer"
        >
            <ModalBody>
                <div className="d-flex  mb-5">
                    <Steps current={currentStep} steps={stepsList} className="flex-grow me-4"></Steps>{" "}
                    <CloseButton onClick={toggleCreateDatabase} />
                </div>
                <FormGroup floating>
                    <Input type="text" placeholder="Database Name" name="Database Name" id="DbName" />
                    <Label for="DbName">Database Name</Label>
                </FormGroup>
            </ModalBody>
            <ModalFooter>
                {firstStep ? (
                    <Button onClick={prevStep} className="rounded-pill">
                        <Icon icon="database" addon="arrow-up" /> Create from backup
                    </Button>
                ) : (
                    <Button onClick={prevStep} className="rounded-pill">
                        <i className="icon-arrow-left" /> Back
                    </Button>
                )}
                <FlexGrow />
                <Button className="rounded-pill me-1" id="QuickCreateButton">
                    Quick Create
                </Button>

                <UncontrolledPopover
                    placement="top"
                    target="QuickCreateButton"
                    trigger="hover"
                    container="PopoverContainer"
                >
                    <PropSummary>
                        <PropSummaryItem>
                            <PropSummaryName>
                                <Icon icon="encryption" className="me-1" /> Encryption
                            </PropSummaryName>
                            <PropSummaryValue color="danger">OFF</PropSummaryValue>
                        </PropSummaryItem>
                        <PropSummaryItem>
                            <PropSummaryName>
                                <Icon icon="replication" className="me-1" /> Replication
                            </PropSummaryName>
                            <PropSummaryValue color="danger">OFF</PropSummaryValue>
                        </PropSummaryItem>
                        <PropSummaryItem>
                            <PropSummaryName>
                                <Icon icon="sharding" className="me-1" /> Sharding
                            </PropSummaryName>
                            <PropSummaryValue color="danger">OFF</PropSummaryValue>
                        </PropSummaryItem>
                        <PropSummaryItem>
                            <PropSummaryName>
                                <Icon icon="path" className="me-1" /> <strong>Default</strong> paths
                            </PropSummaryName>
                        </PropSummaryItem>
                    </PropSummary>
                </UncontrolledPopover>

                {lastStep ? (
                    <Button color="success" className="rounded-pill">
                        <i className="icon-rocket me-1" /> Finish
                    </Button>
                ) : (
                    <Button color="primary" className="rounded-pill" onClick={nextStep} disabled={lastStep}>
                        Next <i className="icon-arrow-right" />
                    </Button>
                )}
            </ModalFooter>
        </Modal>
    );
}

import React, { ReactFragment, useState } from "react";
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
import { active } from "sortablejs";
import { Checkbox } from "./Checkbox";

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

    const [manualNodeSelection, setManualNodeSelection] = useState(false);

    const [encryption, setEncryption] = useState(false);
    const [replication, setReplication] = useState(false);
    const [sharding, setSharding] = useState(false);

    const enableEncryption = () => {
        setEncryption(true);
    };

    const disableEncryption = () => {
        setEncryption(false);
    };

    const toggleManualNodeSelection = () => {
        setManualNodeSelection(!manualNodeSelection);
        console.log(activeSteps);
    };

    const stepsList = [
        { stepName: "New from backup", view: <StepCreateFromBackup />, active: false },
        { stepName: "Setup", view: <StepBasicSetup />, active: true },
        {
            stepName: "Encryption",
            view: (
                <StepEncryption
                    encryption={encryption}
                    enableEncryption={enableEncryption}
                    disableEncryption={disableEncryption}
                />
            ),
            active: true,
        },
        {
            stepName: "Replication & Sharding",
            view: (
                <StepReplicationSharding
                    manualNodeSelection={manualNodeSelection}
                    toggleManualNodeSelection={toggleManualNodeSelection}
                />
            ),
            active: true,
        },
        { stepName: "Manual Node Selection", view: <StepNodeSelection />, active: { manualNodeSelection } },
        { stepName: "Paths Configuration", view: <StepPaths />, active: true },
    ];

    const activeSteps = stepsList.filter((step) => step.active === true);

    const isLastStep = activeSteps.length - 2 < currentStep ? true : false;
    const isFirstStep = currentStep < 1 ? true : false;

    const nextStep = () => {
        if (!isLastStep) setCurrentStep(currentStep + 1);
    };

    const prevStep = () => {
        if (!isFirstStep) setCurrentStep(currentStep - 1);
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
                    <Steps
                        current={currentStep}
                        steps={stepsList.filter((step) => step.active === true).map((step) => step.stepName)}
                        className="flex-grow me-4"
                    ></Steps>
                    <CloseButton onClick={toggleCreateDatabase} />
                </div>

                <RenderSetupSteps currentStep={currentStep} stepsList={activeSteps.map((step) => step.view)} />
            </ModalBody>
            <ModalFooter>
                {isFirstStep ? (
                    <Button onClick={prevStep} className="rounded-pill">
                        <Icon icon="database" addon="arrow-up me-1" /> Create from backup
                    </Button>
                ) : (
                    <Button onClick={prevStep} className="rounded-pill">
                        <Icon icon="arrow-thin-left" className="me-1" /> Back
                    </Button>
                )}
                <FlexGrow />
                {!isLastStep && (
                    <>
                        <Button className="rounded-pill me-1" id="QuickCreateButton">
                            <Icon icon="star" className="me-1" />
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
                                    {encryption ? (
                                        <PropSummaryValue color="success"> ON</PropSummaryValue>
                                    ) : (
                                        <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                                    )}
                                </PropSummaryItem>

                                <PropSummaryItem>
                                    <PropSummaryName>
                                        <Icon icon="replication" className="me-1" /> Replication
                                    </PropSummaryName>
                                    {replication ? (
                                        <PropSummaryValue color="success"> ON</PropSummaryValue>
                                    ) : (
                                        <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                                    )}
                                </PropSummaryItem>

                                <PropSummaryItem>
                                    <PropSummaryName>
                                        <Icon icon="sharding" className="me-1" /> Sharding
                                    </PropSummaryName>
                                    {sharding ? (
                                        <PropSummaryValue color="success"> ON</PropSummaryValue>
                                    ) : (
                                        <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                                    )}
                                </PropSummaryItem>

                                {manualNodeSelection && (
                                    <PropSummaryItem>
                                        <PropSummaryName>
                                            <Icon icon="node" className="me-1" /> Manual node
                                        </PropSummaryName>
                                        <PropSummaryValue color="success"> ON</PropSummaryValue>
                                    </PropSummaryItem>
                                )}

                                <PropSummaryItem>
                                    <PropSummaryName>
                                        <Icon icon="path" className="me-1" /> <strong>Default</strong> paths
                                    </PropSummaryName>
                                </PropSummaryItem>
                            </PropSummary>
                        </UncontrolledPopover>
                    </>
                )}

                {isLastStep ? (
                    <Button color="success" className="rounded-pill">
                        <Icon icon="rocket" className="me-1" /> Finish
                    </Button>
                ) : (
                    <Button color="primary" className="rounded-pill" onClick={nextStep} disabled={isLastStep}>
                        Next <Icon icon="arrow-thin-right" className="ms-1" />
                    </Button>
                )}
            </ModalFooter>
        </Modal>
    );
}
interface RenderSetupStepsProps {
    stepsList: JSX.Element[];
    currentStep: number;
}

export function RenderSetupSteps(props: RenderSetupStepsProps) {
    const { stepsList, currentStep } = props;
    return stepsList[currentStep];
}

interface StepBasicSetupProps {}

export function StepBasicSetup(props: StepBasicSetupProps) {
    return (
        <FormGroup floating>
            <Input type="text" placeholder="Database Name" name="Database Name" id="DbName" />
            <Label for="DbName">Database Name</Label>
        </FormGroup>
    );
}

interface StepEncryptionProps {
    encryption: boolean;
    enableEncryption: () => void;
    disableEncryption: () => void;
}

export function StepEncryption(props: StepEncryptionProps) {
    const { encryption, enableEncryption, disableEncryption } = props;
    return (
        <div>
            <h2 className="text-center">Encrypt database?</h2>

            <div className="d-flex justify-content-center">
                <Button active={!encryption} onClick={disableEncryption} outline className="rounded-pill me-2">
                    <Icon icon="unencrypted" /> Don't Encrypt
                </Button>
                <Button active={encryption} onClick={enableEncryption} color="success" outline className="rounded-pill">
                    <Icon icon="encryption" /> Encrypt
                </Button>
            </div>
        </div>
    );
}

interface StepReplicationShardingProps {
    manualNodeSelection: boolean;
    toggleManualNodeSelection: () => void;
}

export function StepReplicationSharding(props: StepReplicationShardingProps) {
    const { manualNodeSelection, toggleManualNodeSelection } = props;
    return (
        <div>
            <h2>Replication & Sharding</h2>
            <Checkbox selected={manualNodeSelection} toggleSelection={toggleManualNodeSelection}>
                Manual node selection
            </Checkbox>
        </div>
    );
}

interface StepNodeSelectionProps {}

export function StepNodeSelection(props: StepNodeSelectionProps) {
    return <h2>Manual Node Selection</h2>;
}

interface StepPathsProps {}

export function StepPaths(props: StepPathsProps) {
    return <h2>Paths Configuration</h2>;
}

interface StepCreateFromBackupProps {}

export function StepCreateFromBackup(props: StepCreateFromBackupProps) {
    return <h2>Restore from backup</h2>;
}

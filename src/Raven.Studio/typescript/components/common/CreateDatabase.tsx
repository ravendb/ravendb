import React, { useState } from "react";

import "./CreateDatabase.scss";

import {
    Alert,
    Button,
    CloseButton,
    Col,
    Collapse,
    FormGroup,
    Input,
    InputGroup,
    Label,
    Modal,
    ModalBody,
    ModalFooter,
    Row,
    UncontrolledPopover,
} from "reactstrap";

import { Checkbox, Switch } from "./Checkbox";
import { FlexGrow } from "./FlexGrow";
import { Icon } from "./Icon";
import { PropSummary, PropSummaryItem, PropSummaryName, PropSummaryValue } from "./PropSummary";
import { Steps } from "./Steps";

interface CreateDatabaseProps {
    createDatabaseModal: boolean;
    toggleCreateDatabase: () => void;
    serverAuthentication: boolean;
}

type StepId = "createFromBackup" | "createNew" | "encryption" | "replicationAndSharding" | "nodeSelection" | "paths";

interface StepItem {
    id: StepId;
    label: string;
    active: boolean;
}

export function CreateDatabase(props: CreateDatabaseProps) {
    const { createDatabaseModal, toggleCreateDatabase, serverAuthentication } = props;
    const [currentStep, setCurrentStep] = useState(0);

    const [manualNodeSelection, setManualNodeSelection] = useState(false);

    const toggleManualNodeSelection = () => {
        setManualNodeSelection(!manualNodeSelection);
    };

    const [encryptionEnabled, setEncryptionEnabled] = useState(false);

    const enableEncryption = () => {
        setEncryptionEnabled(true);
    };

    const disableEncryption = () => {
        setEncryptionEnabled(false);
    };

    const [replicationEnabled, setReplicationEnabled] = useState(false);

    const [shardingEnabled, setShardingEnabled] = useState(false);

    const toggleSharding = () => {
        setShardingEnabled(!shardingEnabled);
        console.log(shardingEnabled);
    };

    const onCreatorClose = () => {
        setCurrentStep(0);
    };

    const [createFromBackup, setCreateFromBackup] = useState(false);

    const toggleCreateFromBackup = () => {
        setCreateFromBackup(!createFromBackup);
    };

    const stepsList: StepItem[] = [
        {
            id: "createFromBackup",
            label: "Select backup",
            active: createFromBackup,
        },
        { id: "createNew", label: "Name", active: !createFromBackup },
        {
            id: "encryption",
            label: "Encryption",
            active: true,
        },
        {
            id: "replicationAndSharding",
            label: "Replication & Sharding",
            active: true,
        },
        {
            id: "nodeSelection",
            label: "Manual Node Selection",
            active: manualNodeSelection,
        },
        { id: "paths", label: "Paths Configuration", active: true },
    ];

    const stepViews = {
        createFromBackup: <StepCreateFromBackup />,
        createNew: <StepCreateNew />,
        encryption: (
            <StepEncryption
                serverAuthentication={serverAuthentication}
                encryptionEnabled={encryptionEnabled}
                enableEncryption={enableEncryption}
                disableEncryption={disableEncryption}
            />
        ),
        replicationAndSharding: (
            <StepReplicationAndSharding
                manualNodeSelection={manualNodeSelection}
                toggleManualNodeSelection={toggleManualNodeSelection}
                shardingEnabled={shardingEnabled}
                toggleSharding={toggleSharding}
            />
        ),
        nodeSelection: <StepNodeSelection />,
        paths: <StepPaths />,
    };

    const activeSteps = stepsList.filter((step) => step.active);

    const isLastStep = activeSteps.length - 2 < currentStep;
    const isFirstStep = currentStep < 1;

    const goToStep = (stepNum: number) => {
        setCurrentStep(stepNum);
    };

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
            onClosed={onCreatorClose}
            className="create-database"
        >
            <ModalBody>
                <div className="d-flex  mb-5">
                    <Steps
                        current={currentStep}
                        steps={activeSteps.map((step) => step.label)}
                        onClick={goToStep}
                        className="flex-grow me-4"
                    ></Steps>
                    <CloseButton onClick={toggleCreateDatabase} />
                </div>

                {stepViews[activeSteps[currentStep].id]}
            </ModalBody>

            <ModalFooter>
                {isFirstStep ? (
                    <Button onClick={toggleCreateFromBackup} className="rounded-pill">
                        {createFromBackup ? (
                            <>
                                <Icon icon="database" addon="star me-1" /> Create new database
                            </>
                        ) : (
                            <>
                                <Icon icon="database" addon="arrow-up me-1" /> Create from backup
                            </>
                        )}
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
                                    {encryptionEnabled ? (
                                        <PropSummaryValue color="success"> ON</PropSummaryValue>
                                    ) : (
                                        <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                                    )}
                                </PropSummaryItem>

                                <PropSummaryItem>
                                    <PropSummaryName>
                                        <Icon icon="replication" className="me-1" /> Replication
                                    </PropSummaryName>
                                    {replicationEnabled ? (
                                        <PropSummaryValue color="success"> ON</PropSummaryValue>
                                    ) : (
                                        <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                                    )}
                                </PropSummaryItem>

                                <PropSummaryItem>
                                    <PropSummaryName>
                                        <Icon icon="sharding" className="me-1" /> Sharding
                                    </PropSummaryName>
                                    {shardingEnabled ? (
                                        <PropSummaryValue color="success"> ON</PropSummaryValue>
                                    ) : (
                                        <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                                    )}
                                </PropSummaryItem>

                                {manualNodeSelection && (
                                    <PropSummaryItem>
                                        <PropSummaryName>
                                            <Icon icon="node" className="me-1" /> Manual node selection
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

interface StepBasicSetupProps {}

export function StepCreateNew(props: StepBasicSetupProps) {
    const newDatabaseImg = require("Content/img/createDatabase/new-database.svg");
    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={newDatabaseImg} alt="" className="step-img" />
            </div>
            <h2 className="text-center mb-4">Create new database</h2>
            <Row>
                <Col sm={{ offset: 2, size: 8 }}>
                    <FormGroup floating>
                        <Input type="text" placeholder="Database Name" name="Database Name" id="DbName" />
                        <Label for="DbName">Database Name</Label>
                    </FormGroup>
                </Col>
            </Row>
        </div>
    );
}

interface StepEncryptionProps {
    serverAuthentication: boolean;
    encryptionEnabled: boolean;
    enableEncryption: () => void;
    disableEncryption: () => void;
}

export function StepEncryption(props: StepEncryptionProps) {
    const { serverAuthentication, encryptionEnabled, enableEncryption, disableEncryption } = props;
    const qrImg = require("Content/img/createDatabase/qr.jpg");
    return (
        <div>
            <h2 className="text-center">Encrypt database?</h2>

            <div className="d-flex justify-content-center">
                <Button
                    active={!encryptionEnabled}
                    onClick={disableEncryption}
                    disabled={!serverAuthentication}
                    outline
                    className="rounded-pill me-2"
                >
                    <Icon icon="unencrypted" /> Don't Encrypt
                </Button>
                <Button
                    active={encryptionEnabled}
                    onClick={enableEncryption}
                    disabled={!serverAuthentication}
                    color="success"
                    outline
                    className="rounded-pill"
                >
                    <Icon icon="encryption" /> Encrypt
                </Button>
            </div>

            {!serverAuthentication ? (
                <Row className="my-4">
                    <Col sm={{ size: 8, offset: 2 }}>
                        <Alert color="info">
                            <p className="lead">
                                <Icon icon="unsecure" /> Authentication is off
                            </p>
                            <p>
                                Database encryption is only possible when authentication is enabled and a server
                                certificate has been defined.
                            </p>
                            <p>
                                For more information go to the <a href="#">certificates page</a>
                            </p>
                        </Alert>
                    </Col>
                </Row>
            ) : (
                <Collapse isOpen={encryptionEnabled}>
                    <Row className="mt-4">
                        <Col>
                            <Alert color="warning" className="d-flex align-items-center">
                                <Icon icon="warning" className="fs-2 me-2" />
                                <div>
                                    Save the key in a safe place. It will not be available again. If you lose this key
                                    you could lose access to your data
                                </div>
                            </Alert>
                            <div className="small-label mb-1">Key (Base64 Encoding)</div>
                            <InputGroup>
                                <Input value="13a5f83gy71ws032nm69" />
                                <Button title="Copy to clipboard">
                                    <Icon icon="copy-to-clipboard" />
                                </Button>
                            </InputGroup>
                            <Row className="mt-2">
                                <Col>
                                    <Button block>Download Encryption key</Button>
                                </Col>
                                <Col>
                                    <Button block>Print encryption key</Button>
                                </Col>
                            </Row>
                        </Col>
                        <Col sm="auto">
                            <img src={qrImg} alt="" className="" />
                        </Col>
                    </Row>
                    {/* TODO validate encryption key saved */}
                    <div className="d-flex justify-content-center mt-4">
                        <Checkbox size="lg" selected={null} toggleSelection={null}>
                            <span className="lead">I have saved the encryption key</span>
                        </Checkbox>
                    </div>
                </Collapse>
            )}
        </div>
    );
}

interface StepReplicationAndShardingProps {
    manualNodeSelection: boolean;
    toggleManualNodeSelection: () => void;
    shardingEnabled: boolean;
    toggleSharding: () => void;
}

export function StepReplicationAndSharding(props: StepReplicationAndShardingProps) {
    const { manualNodeSelection, toggleManualNodeSelection, shardingEnabled, toggleSharding } = props;
    return (
        <div>
            <h2>Replication & Sharding</h2>
            <Checkbox selected={manualNodeSelection} toggleSelection={toggleManualNodeSelection}>
                Manual node selection
            </Checkbox>
            <Switch color="shard" selected={shardingEnabled} toggleSelection={toggleSharding}>
                Enable sharding
            </Switch>
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
function componentDidMount() {
    throw new Error("Function not implemented.");
}

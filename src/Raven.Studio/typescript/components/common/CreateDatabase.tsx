import React, { useState } from "react";

import "./CreateDatabase.scss";

import {
    Alert,
    Button,
    CloseButton,
    Col,
    Collapse,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    FormGroup,
    Input,
    InputGroup,
    Label,
    Modal,
    ModalBody,
    ModalFooter,
    PopoverBody,
    Row,
    UncontrolledDropdown,
    UncontrolledPopover,
    UncontrolledTooltip,
} from "reactstrap";

import { Checkbox, Switch } from "./Checkbox";
import { FlexGrow } from "./FlexGrow";
import { Icon } from "./Icon";
import { PropSummary, PropSummaryItem, PropSummaryName, PropSummaryValue } from "./PropSummary";
import { Steps } from "./Steps";
import { LicenseRestrictions } from "./LicenseRestrictions";

interface CreateDatabaseProps {
    createDatabaseModal: boolean;
    toggleCreateDatabase: () => void;
    serverAuthentication: boolean;
    licenseProps: licenseProps;
}

type licenseProps = {
    replication: boolean;
    sharding: boolean;
    dynamicDatabaseDistribution: boolean;
};

type StepId =
    | "createFromBackup"
    | "backupSource"
    | "createNew"
    | "encryption"
    | "replicationAndSharding"
    | "nodeSelection"
    | "paths";

interface StepItem {
    id: StepId;
    label: string;
    active: boolean;
}

export function CreateDatabase(props: CreateDatabaseProps) {
    const { createDatabaseModal, toggleCreateDatabase, serverAuthentication, licenseProps } = props;

    //ENCRYPTION

    const [encryptionEnabled, setEncryptionEnabled] = useState(false);

    const enableEncryption = () => {
        setEncryptionEnabled(true);
    };

    const disableEncryption = () => {
        setEncryptionEnabled(false);
    };

    //REPLICATION

    const [replicationEnabled, setReplicationEnabled] = useState(false);

    const [shardingEnabled, setShardingEnabled] = useState(false);

    const toggleSharding = () => {
        setShardingEnabled(!shardingEnabled);
        console.log(shardingEnabled);
    };

    const [manualNodeSelection, setManualNodeSelection] = useState(false);

    const toggleManualNodeSelection = () => {
        setManualNodeSelection(!manualNodeSelection);
    };

    //BACKUP

    const [createFromBackup, setCreateFromBackup] = useState(false);
    const [backupSource, setBackupSource] = useState(null);

    const toggleCreateFromBackup = () => {
        setCreateFromBackup(!createFromBackup);
    };

    //NAVIGATION

    const [currentStep, setCurrentStep] = useState(0);

    const stepsList: StepItem[] = [
        {
            id: "createFromBackup",
            label: "Select backup",
            active: createFromBackup,
        },
        { id: "backupSource", label: "Backup Source", active: createFromBackup },
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

    const activeSteps = stepsList.filter((step) => step.active);

    const isLastStep = activeSteps.length - 2 < currentStep;
    const isFirstStep = currentStep < 1;
    const showQuickCreate = !isLastStep && !createFromBackup;

    const goToStep = (stepNum: number) => {
        setCurrentStep(stepNum);
    };

    const nextStep = () => {
        if (!isLastStep) setCurrentStep(currentStep + 1);
    };

    const prevStep = () => {
        if (!isFirstStep) setCurrentStep(currentStep - 1);
    };

    const onCreatorClose = () => {
        setCurrentStep(0);
    };

    const stepViews = {
        createFromBackup: (
            <StepCreateFromBackup
                shardingEnabled={shardingEnabled}
                setShardingEnabled={setShardingEnabled}
                licenseIncludesSharding={licenseProps.sharding}
            />
        ),
        backupSource: <StepBackupSource backupSource={backupSource} setBackupSource={setBackupSource} />,
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
                setShardingEnabled={setShardingEnabled}
                toggleSharding={toggleSharding}
                licenseIncludesSharding={licenseProps.sharding}
            />
        ),
        nodeSelection: <StepNodeSelection />,
        paths: <StepPaths />,
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
                {showQuickCreate && (
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
    const encryptionImg = require("Content/img/createDatabase/encryption.svg");
    const qrImg = require("Content/img/createDatabase/qr.jpg");
    return (
        <div>
            <Collapse isOpen={!encryptionEnabled}>
                <div className="d-flex justify-content-center">
                    <img src={encryptionImg} alt="" className="step-img" />
                </div>
            </Collapse>
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
                            <Alert color="warning" className="d-flex align-items-center mt-2">
                                <Icon icon="warning" className="fs-2 me-2" />
                                <div>
                                    Save the key in a safe place. It will not be available again. If you lose this key
                                    you could lose access to your data
                                </div>
                            </Alert>
                        </Col>
                        <Col sm="auto">
                            <img src={qrImg} alt="" className="" />
                        </Col>
                    </Row>
                    {/* TODO validate encryption key saved */}
                    <div className="d-flex justify-content-center mt-3">
                        <Checkbox size="lg" color="primary" selected={null} toggleSelection={null}>
                            <span className="lead ms-2">I have saved the encryption key</span>
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
    setShardingEnabled: (value: boolean) => void;
    toggleSharding: () => void;
    licenseIncludesSharding: boolean;
}

export function StepReplicationAndSharding(props: StepReplicationAndShardingProps) {
    const {
        manualNodeSelection,
        toggleManualNodeSelection,
        shardingEnabled,
        toggleSharding,
        setShardingEnabled,
        licenseIncludesSharding,
    } = props;

    const shardingImg = require("Content/img/createDatabase/sharding.svg");

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={shardingImg} alt="" className="step-img" />
            </div>
            <h2 className="text-center">Replication & Sharding</h2>

            <div className="text-center mt-3">
                <span>
                    Available nodes: <Icon icon="node" color="node" className="ms-1" /> <strong>5</strong>
                </span>
                <span id="ShardingInfo" className="ms-5">
                    <Icon icon="info" color="info" /> What is sharding?
                </span>
            </div>

            <UncontrolledPopover target="ShardingInfo" placement="top" trigger="hover" container="PopoverContainer">
                <PopoverBody>
                    <p>
                        <strong className="text-shard">
                            <Icon icon="sharding" /> Sharding
                        </strong>{" "}
                        is a database partitioning technique that breaks up large databases into smaller, more
                        manageable pieces called{" "}
                        <strong className="text-shard">
                            {" "}
                            <Icon icon="shard" />
                            shards
                        </strong>
                        .
                    </p>
                    <p>
                        Each shard contains a subset of the data and can be stored on a separate server, allowing for{" "}
                        <strong>horizontal scalability and improved performance</strong>.
                    </p>
                    <a href="#">
                        Learn more TODO <Icon icon="newtab" />
                    </a>
                </PopoverBody>
            </UncontrolledPopover>

            <Switch color="shard" selected={shardingEnabled} toggleSelection={toggleSharding}>
                Enable sharding
            </Switch>
            <div className="d-flex justify-content-center mt-4">
                <Button
                    active={!shardingEnabled}
                    onClick={() => setShardingEnabled(false)}
                    outline
                    className="rounded-pill me-2"
                >
                    <Icon icon="node" className="me-1" /> Unsharded
                </Button>
                <Button
                    active={shardingEnabled}
                    onClick={() => setShardingEnabled(true)}
                    color="shard"
                    outline
                    className="rounded-pill"
                >
                    <Icon icon="sharding" className="me-1" /> Sharded
                </Button>
            </div>

            <Collapse isOpen={true}>
                <Row>
                    <Col>
                        <LicenseRestrictions isAvailable={false} featureName="Dynamic database distribution">
                            <Switch color="primary" selected={null} toggleSelection={null} disabled={true}>
                                Allow dynamic database distribution
                                <br />
                                <small>Maintain replication factor upon node failure</small>
                            </Switch>
                        </LicenseRestrictions>
                    </Col>
                    <Col>
                        <Switch
                            color="primary"
                            selected={manualNodeSelection}
                            toggleSelection={toggleManualNodeSelection}
                        >
                            Set replication nodes manually
                            <br />
                            <small>Select nodes from the list in the next step</small>
                        </Switch>
                    </Col>
                </Row>
            </Collapse>
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

interface StepCreateFromBackupProps {
    licenseIncludesSharding: boolean;
    shardingEnabled: boolean;
    setShardingEnabled: (value: boolean) => void;
}

export function StepCreateFromBackup(props: StepCreateFromBackupProps) {
    const { shardingEnabled, setShardingEnabled, licenseIncludesSharding } = props;
    const fromBackupImg = require("Content/img/createDatabase/from-backup.svg");

    return (
        <>
            <div className="d-flex justify-content-center">
                <img src={fromBackupImg} alt="" className="step-img" />
            </div>

            <h2 className="text-center mb-4">Restore from backup</h2>
            <Row>
                <Col sm={{ offset: 2, size: 8 }}>
                    <FormGroup floating>
                        <Input type="text" placeholder="Database Name" name="Database Name" id="DbName" />
                        <Label for="DbName">Database Name</Label>
                    </FormGroup>

                    <div className="d-flex justify-content-center mt-4">
                        <Button
                            active={!shardingEnabled}
                            onClick={() => setShardingEnabled(false)}
                            outline
                            className="rounded-pill me-2"
                        >
                            <Icon icon="node" className="me-1" /> Unsharded
                        </Button>

                        <LicenseRestrictions
                            isAvailable={licenseIncludesSharding}
                            featureName={
                                <strong className="text-shard">
                                    <Icon icon="sharding" /> Sharding
                                </strong>
                            }
                        >
                            <Button
                                active={shardingEnabled}
                                onClick={() => setShardingEnabled(true)}
                                color="shard"
                                outline
                                className="rounded-pill"
                                disabled={!licenseIncludesSharding}
                            >
                                <Icon icon="sharding" className="me-1" /> Sharded
                            </Button>
                        </LicenseRestrictions>
                    </div>
                </Col>
            </Row>
        </>
    );
}

interface StepBackupSourceProps {
    backupSource: string;
    setBackupSource: (source: string) => void;
}

export function StepBackupSource(props: StepBackupSourceProps) {
    const { backupSource, setBackupSource } = props;

    const backupSourceImg = require("Content/img/createDatabase/backup-source.svg");

    const getSourceName = (source: string) => {
        switch (source) {
            case "local":
                return (
                    <>
                        <Icon icon="storage" className="me-1" />
                        Local Server Directory
                    </>
                );
            case "cloud":
                return (
                    <>
                        <Icon icon="cloud" className="me-1" /> RavenDB Cloud
                    </>
                );
            case "aws":
                return (
                    <>
                        <Icon icon="aws" className="me-1" /> Amazon S3
                    </>
                );
            case "azure":
                return (
                    <>
                        <Icon icon="azure" className="me-1" /> Microsoft Azure
                    </>
                );
            case "gcp":
                return (
                    <>
                        <Icon icon="gcp" className="me-1" />
                        Google Cloud Platform
                    </>
                );
        }
    };
    return (
        <>
            <Collapse isOpen={backupSource === null}>
                <div className="d-flex justify-content-center">
                    <img src={backupSourceImg} alt="" className="step-img" />
                </div>
            </Collapse>
            <h2 className="text-center">Backup Source</h2>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Backup Source</Label>
                </Col>
                <Col>
                    <UncontrolledDropdown>
                        <DropdownToggle caret className="w-100">
                            {backupSource ? getSourceName(backupSource) : "Select"}
                        </DropdownToggle>
                        <DropdownMenu className="w-100">
                            <DropdownItem onClick={() => setBackupSource("local")}>
                                {getSourceName("local")}
                            </DropdownItem>
                            <DropdownItem onClick={() => setBackupSource("cloud")}>
                                {getSourceName("cloud")}
                            </DropdownItem>
                            <DropdownItem onClick={() => setBackupSource("aws")}>{getSourceName("aws")}</DropdownItem>
                            <DropdownItem onClick={() => setBackupSource("azure")}>
                                {getSourceName("azure")}
                            </DropdownItem>
                            <DropdownItem onClick={() => setBackupSource("gcp")}>{getSourceName("gcp")}</DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                </Col>
            </Row>
            <Collapse isOpen={backupSource != null}>
                <>
                    {backupSource === "local" && backupSourceFragmentLocal()}
                    {backupSource === "cloud" && backupSourceFragmentCloud()}
                    {backupSource === "aws" && backupSourceFragmentAws()}
                    {backupSource === "azure" && backupSourceFragmentAzure()}
                    {backupSource === "gcp" && backupSourceFragmentGcp()}
                    <Row className="mt-2">
                        <Col sm="3">
                            <Label className="col-form-label">Restore Point</Label>
                        </Col>
                        <Col>
                            {backupSource && (
                                <>
                                    <UncontrolledDropdown>
                                        <DropdownToggle caret className="w-100">
                                            Select
                                        </DropdownToggle>
                                        <DropdownMenu className="w-100">
                                            <DropdownItem>TODO: add restore point selection</DropdownItem>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                    <div className="mt-4">
                                        <Switch color="primary" selected={null} toggleSelection={null}>
                                            Disable ongoing tasks after restore
                                        </Switch>
                                        <Switch color="primary" selected={null} toggleSelection={null}>
                                            Skip indexes
                                        </Switch>
                                    </div>
                                </>
                            )}
                        </Col>
                    </Row>
                </>
            </Collapse>
        </>
    );
}

export function backupSourceFragmentLocal() {
    return (
        <div className="mt-2">
            <Row>
                <Col sm="3">
                    <label className="col-form-label">Directory Path</label>
                </Col>
                <Col>
                    <Input placeholder="Enter backup directory path"></Input>
                </Col>
            </Row>
        </div>
    );
}

export function backupSourceFragmentCloud() {
    return (
        <div className="mt-2">
            <Row>
                <Col sm="3">
                    <Label className="col-form-label" id="CloudBackupLinkInfo">
                        Backup Link <Icon icon="info" color="info" />
                    </Label>
                </Col>
                <Col>
                    <Input placeholder="Enter backup link generated in RavenDB Cloud"></Input>
                </Col>
            </Row>
            <UncontrolledPopover target="CloudBackupLinkInfo" trigger="hover" container="PopoverContainer">
                <PopoverBody>
                    <ol className="m-0">
                        <li>
                            Login to your <code>RavenDB Cloud Account</code>
                        </li>
                        <li>
                            Go to <code>Backups</code> view
                        </li>
                        <li>Select desired Instance and a Backup File</li>
                        <li>
                            Click <code>Generate Backup Link</code>
                        </li>
                    </ol>
                </PopoverBody>
            </UncontrolledPopover>
        </div>
    );
}

export function backupSourceFragmentAws() {
    const [useCustomHost, setUseCustomHost] = useState(false);
    const toggleUseCustomHost = () => {
        setUseCustomHost(!useCustomHost);
    };
    return (
        <div className="mt-2">
            <Row>
                <Col sm={{ offset: 3 }}>
                    <Switch color="primary" selected={useCustomHost} toggleSelection={toggleUseCustomHost}>
                        Use a custom S3 host
                    </Switch>
                </Col>
            </Row>

            <Collapse isOpen={useCustomHost}>
                <Row>
                    <Col sm={{ offset: 3 }}>
                        <Switch color="primary" selected={null} toggleSelection={null}>
                            Force path style <Icon icon="info" className="text-info" id="CloudBackupLinkInfo" />
                        </Switch>
                        <UncontrolledPopover target="CloudBackupLinkInfo" trigger="hover" container="PopoverContainer">
                            <PopoverBody>
                                Whether to force path style URLs for S3 objects (e.g.,{" "}
                                <code>https://&#123;Server-URL&#125;/&#123;Bucket-Name&#125;</code> instead of{" "}
                                <code>https://&#123;Bucket-Name&#125;.&#123;Server-URL&#125;</code>)
                            </PopoverBody>
                        </UncontrolledPopover>
                    </Col>
                </Row>
                <Row className="mt-2">
                    <Col sm="3">
                        <Label className="col-form-label">Custom server URL</Label>
                    </Col>
                    <Col>
                        <Input />
                    </Col>
                </Row>
            </Collapse>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Secret key</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Aws Region</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Name</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Bucket Name</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">
                        Remote Folder Name <small>(optional)</small>
                    </Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>
        </div>
    );
}

export function backupSourceFragmentAzure() {
    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Account Name</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Account Key</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Container</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">
                        Remote Folder Name <small>(optional)</small>
                    </Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>
        </div>
    );
}

export function backupSourceFragmentGcp() {
    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Bucket Name</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">Google Credentials</Label>
                </Col>
                <Col>
                    <Input
                        type="textarea"
                        rows="18"
                        placeholder='e.g.

{
    "type": "service_account",
    "project_id": "test-raven-237012",
    "private_key_id": "12345678123412341234123456789101",
    "private_key": "-----BEGIN PRIVATE KEY-----\abCse=\n-----END PRIVATE KEY-----\n",
    "client_email": "raven@test-raven-237012-237012.iam.gserviceaccount.com",
    "client_id": "111390682349634407434",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/viewonly%40test-raven-237012.iam.gserviceaccount.com"
}'
                    />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col sm="3">
                    <Label className="col-form-label">
                        Remote Folder Name <small>(optional)</small>
                    </Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>
        </div>
    );
}

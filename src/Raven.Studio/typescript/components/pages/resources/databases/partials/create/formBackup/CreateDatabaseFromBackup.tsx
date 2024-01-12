import React, { useState } from "react";
import {
    Alert,
    Button,
    ButtonGroup,
    CloseButton,
    Col,
    Collapse,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    FormGroup,
    Input,
    InputGroup,
    InputGroupText,
    Label,
    ModalBody,
    ModalFooter,
    PopoverBody,
    Row,
    Table,
    UncontrolledDropdown,
    UncontrolledPopover,
} from "reactstrap";
import { Checkbox, Switch } from "../../../../../../common/Checkbox";
import { FlexGrow } from "../../../../../../common/FlexGrow";
import { Icon } from "../../../../../../common/Icon";
import { Steps } from "../../../../../../common/Steps";
import { LicenseRestrictions } from "../../../../../../common/LicenseRestrictions";
import classNames from "classnames";

interface CreateDatabaseFromBackupProps {
    closeModal: () => void;
    changeCreateModeToRegular: () => void;
}

type licenseProps = {
    encryption: boolean;
    sharding: boolean;
    dynamicDatabaseDistribution: boolean;
};

type StepId = "createFromBackup" | "backupSource" | "encryption" | "paths";

interface StepItem {
    id: StepId;
    label: string;
    active: boolean;
}

export default function CreateDatabaseFromBackup(props: CreateDatabaseFromBackupProps) {
    const { closeModal, changeCreateModeToRegular } = props;

    const isServerAuthenticationEnabled = true; // TODO get from store
    const [encryptionEnabled, setEncryptionEnabled] = useState(false);

    const toggleEncryption = () => {
        setEncryptionEnabled(!encryptionEnabled);
    };

    const nodeList: databaseLocationSpecifier[] = [
        { nodeTag: "A" },
        { nodeTag: "B" },
        { nodeTag: "C" },
        { nodeTag: "D" },
        { nodeTag: "DEV" },
    ];

    const [shardingEnabled, setShardingEnabled] = useState(false);
    const [backupSource, setBackupSource] = useState(null);

    const [useDefaultPaths, setUseDefaultPaths] = useState(true);
    const toggleUseDefaultPaths = () => {
        setUseDefaultPaths(!useDefaultPaths);
    };

    const [currentStep, setCurrentStep] = useState(0);

    const stepsList: StepItem[] = [
        {
            id: "createFromBackup",
            label: "Select backup",
            active: true,
        },
        { id: "backupSource", label: "Backup Source", active: true },
        {
            id: "encryption",
            label: "Encryption",
            active: encryptionEnabled,
        },
        { id: "paths", label: "Paths Configuration", active: true },
    ];

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

    const stepViews: Record<StepId, any> = {
        createFromBackup: (
            <StepCreateFromBackup
                shardingEnabled={shardingEnabled}
                setShardingEnabled={setShardingEnabled}
                licenseIncludesSharding={true}
            />
        ),
        backupSource: (
            <StepBackupSource
                backupSource={backupSource}
                setBackupSource={setBackupSource}
                shardingEnabled={shardingEnabled}
                nodeList={nodeList.map((node) => node.nodeTag)}
                encryptionEnabled={encryptionEnabled}
                toggleEncryption={toggleEncryption}
                licenseProps={null}
                serverAuthentication={isServerAuthenticationEnabled}
            />
        ),
        encryption: <StepEncryption />,
        paths: (
            <StepPaths
                nodeList={nodeList.map((node) => node.nodeTag)}
                useDefaultPaths={useDefaultPaths}
                toggleUseDefaultPaths={toggleUseDefaultPaths}
            />
        ),
    };

    return (
        <>
            <ModalBody>
                <div className="d-flex  mb-5">
                    <Steps
                        current={currentStep}
                        steps={activeSteps.map((step) => step.label)}
                        onClick={goToStep}
                        className="flex-grow me-4"
                    ></Steps>
                    <CloseButton onClick={closeModal} />
                </div>
                {stepViews[activeSteps[currentStep].id]}
            </ModalBody>

            <ModalFooter>
                {isFirstStep ? (
                    <Button onClick={changeCreateModeToRegular} className="rounded-pill">
                        <Icon icon="database" addon="arrow-up" /> Create from backup
                    </Button>
                ) : (
                    <Button onClick={prevStep} className="rounded-pill">
                        <Icon icon="arrow-thin-left" /> Back
                    </Button>
                )}
                <FlexGrow />
                {isLastStep ? (
                    <Button color="success" className="rounded-pill">
                        <Icon icon="rocket" /> Finish
                    </Button>
                ) : (
                    <Button color="primary" className="rounded-pill" onClick={nextStep} disabled={isLastStep}>
                        Next <Icon icon="arrow-thin-right" margin="ms-1" />
                    </Button>
                )}
            </ModalFooter>
        </>
    );
}

function StepEncryption() {
    const encryptionImg = require("Content/img/createDatabase/encryption.svg");
    const qrImg = require("Content/img/createDatabase/qr.jpg");
    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={encryptionImg} alt="" className="step-img" />
            </div>

            <h2 className="text-center">Encryption at Rest</h2>

            <Row className="mt-4">
                <Col>
                    <div className="small-label mb-1">Key (Base64 Encoding)</div>
                    <InputGroup>
                        <Input value="13a5f83gy71ws032nm69" />
                        <Button title="Copy to clipboard">
                            <Icon icon="copy-to-clipboard" margin="m-0" />
                        </Button>
                    </InputGroup>
                    <Row className="mt-2">
                        <Col>
                            <Button block color="primary" size="sm">
                                <Icon icon="download" /> Download encryption key
                            </Button>
                        </Col>
                        <Col>
                            <Button block size="sm">
                                <Icon icon="print" /> Print encryption key
                            </Button>
                        </Col>
                    </Row>
                    <Alert color="warning" className="d-flex align-items-center mt-2">
                        <Icon icon="warning" margin="me-2" className="fs-2" />
                        <div>
                            Save the key in a safe place. It will not be available again. If you lose this key you could
                            lose access to your data
                        </div>
                    </Alert>
                </Col>
                <Col lg="auto" className="text-center">
                    <img src={qrImg} alt="" />
                    <div className="text-center mt-1">
                        <small id="qrInfo" className="text-info">
                            <Icon icon="info" margin="m-0" /> what&apos;s this?
                        </small>
                    </div>
                    <UncontrolledPopover target="qrInfo" placement="top" trigger="hover" container="PopoverContainer">
                        <PopoverBody>TODO: write info about qr code</PopoverBody>
                    </UncontrolledPopover>
                </Col>
            </Row>
            {/* TODO validate encryption key saved */}
            <div className="d-flex justify-content-center mt-3">
                <Checkbox size="lg" color="primary" selected={null} toggleSelection={null}>
                    <span className="lead ms-2">I have saved the encryption key</span>
                </Checkbox>
            </div>
        </div>
    );
}

interface StepPathsProps {
    nodeList: string[];
    useDefaultPaths: boolean;
    toggleUseDefaultPaths: () => void;
}

function StepPaths(props: StepPathsProps) {
    const { nodeList, useDefaultPaths, toggleUseDefaultPaths } = props;

    return (
        <div>
            <h2 className="text-center">Paths Configuration</h2>
            <InputGroup className="my-4">
                <InputGroupText>
                    <Checkbox selected={useDefaultPaths} toggleSelection={toggleUseDefaultPaths}>
                        Use default paths
                    </Checkbox>
                </InputGroupText>
                <Input disabled={useDefaultPaths} value="data/test"></Input>
            </InputGroup>
            <Table responsive className="m-0">
                <thead>
                    <tr>
                        <th />
                        <th>Path</th>
                        <th>Free space</th>
                        <th>Total</th>
                    </tr>
                </thead>
                <tbody>
                    {nodeList.map((nodeTag) => (
                        <tr>
                            <th scope="row" className="align-middle">
                                <strong>
                                    <Icon icon="node" color="node" margin="m-0" /> {nodeTag}
                                </strong>
                            </th>
                            <td className="align-middle text-break">/data/test</td>
                            <td className="align-middle">24.45 GBytes</td>
                            <td className="align-middle">29.40 GBytes</td>
                        </tr>
                    ))}
                </tbody>
            </Table>
        </div>
    );
}

interface StepCreateFromBackupProps {
    licenseIncludesSharding: boolean;
    shardingEnabled: boolean;
    setShardingEnabled: (value: boolean) => void;
}

function StepCreateFromBackup(props: StepCreateFromBackupProps) {
    const { shardingEnabled, setShardingEnabled, licenseIncludesSharding } = props;
    const fromBackupImg = require("Content/img/createDatabase/from-backup.svg");

    return (
        <>
            <div className="d-flex justify-content-center">
                <img src={fromBackupImg} alt="" className="step-img" />
            </div>

            <h2 className="text-center mb-4">Restore from backup</h2>

            <Row>
                <Col lg={{ offset: 2, size: 8 }}>
                    <FormGroup floating>
                        <Input type="text" placeholder="Database Name" name="Database Name" id="DbName" />
                        <Label for="DbName">Database Name</Label>
                    </FormGroup>
                </Col>
            </Row>

            <Row>
                <Col sm="6" lg={{ offset: 2, size: 4 }}>
                    <Button
                        active={!shardingEnabled}
                        onClick={() => setShardingEnabled(false)}
                        outline
                        className=" me-2 px-4 pt-3 w-100"
                        color="node"
                    >
                        <Icon icon="database" margin="m-0" className="fs-2" />
                        <br />
                        Regular database
                    </Button>
                </Col>
                <Col sm="6" lg="4">
                    <LicenseRestrictions
                        isAvailable={licenseIncludesSharding}
                        featureName={
                            <strong className="text-shard">
                                <Icon icon="sharding" margin="m-0" /> Sharding
                            </strong>
                        }
                        className="d-inline-block"
                    >
                        <Button
                            active={shardingEnabled}
                            onClick={() => setShardingEnabled(true)}
                            color="shard"
                            outline
                            className="px-4 pt-3 w-100"
                            disabled={!licenseIncludesSharding}
                        >
                            <Icon icon="sharding" margin="m-0" className="fs-2" />
                            <br />
                            Sharded database
                        </Button>
                    </LicenseRestrictions>
                </Col>
            </Row>
        </>
    );
}

interface StepBackupSourceProps {
    backupSource: string;
    setBackupSource: (source: string) => void;
    shardingEnabled: boolean;
    nodeList: string[];
    encryptionEnabled: boolean;
    toggleEncryption: () => void;
    licenseProps: licenseProps;
    serverAuthentication: boolean;
}

function StepBackupSource(props: StepBackupSourceProps) {
    const {
        backupSource,
        setBackupSource,
        shardingEnabled,
        nodeList,
        encryptionEnabled,
        toggleEncryption,
        licenseProps,
        serverAuthentication,
    } = props;

    const backupSourceImg = require("Content/img/createDatabase/backup-source.svg");

    const getSourceName = (source: string) => {
        switch (source) {
            case "local":
                return (
                    <>
                        <Icon icon="storage" />
                        Local Server Directory
                    </>
                );
            case "cloud":
                return (
                    <>
                        <Icon icon="cloud" />
                        RavenDB Cloud
                    </>
                );
            case "aws":
                return (
                    <>
                        <Icon icon="aws" />
                        Amazon S3
                    </>
                );
            case "azure":
                return (
                    <>
                        <Icon icon="azure" />
                        Microsoft Azure
                    </>
                );
            case "gcp":
                return (
                    <>
                        <Icon icon="gcp" />
                        Google Cloud Platform
                    </>
                );
            default:
                throw new Error("Unhandled backup source: " + source);
        }
    };
    return (
        <>
            <Collapse isOpen={!backupSource}>
                <div className="d-flex justify-content-center">
                    <img src={backupSourceImg} alt="" className="step-img" />
                </div>
            </Collapse>
            <h2 className="text-center">Backup Source</h2>

            <Row className="mt-2">
                <Col lg="3">
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
            <Collapse isOpen={!!backupSource}>
                <>
                    {backupSource === "local" && (
                        <BackupSourceFragmentLocal shardingEnabled={shardingEnabled} nodeList={nodeList} />
                    )}
                    {backupSource === "cloud" && BackupSourceFragmentCloud()}
                    {backupSource === "aws" && BackupSourceFragmentAws()}
                    {backupSource === "azure" && BackupSourceFragmentAzure()}
                    {backupSource === "gcp" && BackupSourceFragmentGcp()}
                    {shardingEnabled ? (
                        <>
                            {backupSource !== "local" && (
                                <>
                                    <Row className="mt-2">
                                        <Col lg="3">
                                            <label className="col-form-label">Restore&nbsp;Points</label>
                                        </Col>
                                        <Col>
                                            <ButtonGroup className="w-100">
                                                <UncontrolledDropdown className="me-1">
                                                    <DropdownToggle caret>
                                                        <Icon icon="node" color="node" />
                                                        <strong>DEV</strong>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        {nodeList.map((node) => (
                                                            <DropdownItem>
                                                                <Icon icon="node" color="node" />
                                                                <strong>{node}</strong>
                                                            </DropdownItem>
                                                        ))}
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                                <UncontrolledDropdown className="me-1">
                                                    <DropdownToggle caret>
                                                        <Icon icon="shard" color="shard" margin="ms-1" />
                                                        <strong>1</strong>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>
                                                            <Icon icon="shard" color="shard" /> <strong>1</strong>
                                                        </DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                                <RestorePointSelector />
                                            </ButtonGroup>
                                        </Col>
                                    </Row>
                                    <Row className="mt-2">
                                        <Col lg={{ offset: 3 }}>
                                            <Button size="sm" outline color="info" className="rounded-pill">
                                                <Icon icon="restore-backup" margin="m-0" /> Add shard restore point
                                            </Button>
                                        </Col>
                                    </Row>
                                </>
                            )}
                        </>
                    ) : (
                        <Row className="mt-2">
                            <Col lg="3">
                                <Label className="col-form-label">Restore Point</Label>
                            </Col>
                            <Col>
                                {backupSource && (
                                    <>
                                        <RestorePointSelector />
                                    </>
                                )}
                            </Col>
                        </Row>
                    )}

                    <Row>
                        <Col lg={{ offset: 3, size: 9 }}>
                            <div className="mt-4">
                                <Switch color="primary" selected={null} toggleSelection={null}>
                                    <Icon icon="ongoing-tasks" addon="cancel" />
                                    Disable ongoing tasks after restore
                                </Switch>
                                <br />
                                <Switch color="primary" selected={null} toggleSelection={null}>
                                    <Icon icon="index" /> Skip indexes
                                </Switch>
                                <br />
                                {/* TODO: Lock encryption when the source file is encrypted */}
                                {licenseProps?.encryption ? (
                                    <LicenseRestrictions
                                        isAvailable={serverAuthentication}
                                        message={
                                            <>
                                                <p className="lead text-warning">
                                                    <Icon icon="unsecure" margin="m-0" /> Authentication is off
                                                </p>
                                                <p>
                                                    <strong>Encription at Rest</strong> is only possible when
                                                    authentication is enabled and a server certificate has been defined.
                                                </p>
                                                <p>
                                                    For more information go to the <a href="#">certificates page</a>
                                                </p>
                                            </>
                                        }
                                        className="d-inline-block"
                                    >
                                        <Switch
                                            color="primary"
                                            selected={encryptionEnabled}
                                            toggleSelection={toggleEncryption}
                                            disabled={!serverAuthentication}
                                        >
                                            <Icon icon="encryption" />
                                            Encrypt at Rest
                                        </Switch>
                                    </LicenseRestrictions>
                                ) : (
                                    <LicenseRestrictions
                                        isAvailable={licenseProps?.encryption}
                                        featureName={
                                            <strong className="text-primary">
                                                <Icon icon="storage" addon="encryption" margin="m-0" /> Storage
                                                encryption
                                            </strong>
                                        }
                                        className="d-inline-block"
                                    >
                                        <Switch
                                            color="primary"
                                            selected={encryptionEnabled}
                                            toggleSelection={toggleEncryption}
                                            disabled={!licenseProps?.encryption}
                                        >
                                            <Icon icon="encryption" />
                                            Encrypt at Rest
                                        </Switch>
                                    </LicenseRestrictions>
                                )}
                            </div>
                        </Col>
                    </Row>
                </>
            </Collapse>
        </>
    );
}

interface RestorPointSelectorProps {
    className?: string;
    restorePoint?: string;
}
function RestorePointSelector(props: RestorPointSelectorProps) {
    const { className, restorePoint } = props;

    return (
        <UncontrolledDropdown className={classNames("flex-grow-1", className)}>
            <DropdownToggle caret className="w-100">
                {restorePoint ? <>{restorePoint}</> : <>Select restore point</>}
            </DropdownToggle>
            <DropdownMenu className="w-100">
                <DropdownItem>TODO: add restore point selection</DropdownItem>
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}

interface BackupSourceFragmentLocalProps {
    shardingEnabled: boolean;
    nodeList: string[];
}

function BackupSourceFragmentLocal(props: BackupSourceFragmentLocalProps) {
    const { shardingEnabled, nodeList } = props;

    type localBackupSource = {
        node: string;
        shard: number;
        directory: string;
        restorePoint: string;
    };

    const [localBackupSources, setLocalBackupSources] = useState<Array<localBackupSource>>([
        { node: "A", shard: 1, directory: "/backups", restorePoint: "2023 February 21st, 11:19 AM, Full Backup" },
    ]);

    const addBackup = () => {
        setLocalBackupSources((localBackupSources) => [
            ...localBackupSources,
            { node: null, shard: null, directory: null, restorePoint: null },
        ]);
    };

    return (
        <>
            {shardingEnabled ? (
                <>
                    <Row className="mt-2">
                        <Col lg="3">
                            <label className="col-form-label">Directory Path & Restore&nbsp;Point</label>
                        </Col>
                        <Col>
                            {localBackupSources.map((backup) => (
                                <>
                                    <InputGroup>
                                        <UncontrolledDropdown>
                                            <DropdownToggle caret>
                                                <Icon icon="node" color="node" /> <strong>{backup.node}</strong>
                                            </DropdownToggle>
                                            <DropdownMenu>
                                                {nodeList.map((node) => (
                                                    <DropdownItem>
                                                        <Icon icon="node" color="node" />
                                                        <strong>{node}</strong>
                                                    </DropdownItem>
                                                ))}
                                            </DropdownMenu>
                                        </UncontrolledDropdown>
                                        <Input placeholder="Enter backup directory path" value={backup.directory} />
                                        <UncontrolledDropdown>
                                            <DropdownToggle caret>
                                                <Icon icon="shard" color="shard" /> <strong>{backup.shard}</strong>
                                            </DropdownToggle>
                                            <DropdownMenu>
                                                <DropdownItem>
                                                    <Icon icon="shard" color="shard" /> <strong>1</strong>
                                                </DropdownItem>
                                            </DropdownMenu>
                                        </UncontrolledDropdown>
                                    </InputGroup>
                                    <RestorePointSelector restorePoint={backup.restorePoint} className="mt-1 mb-3" />
                                </>
                            ))}
                        </Col>
                    </Row>
                    <Row className="mt-2">
                        <Col lg={{ offset: 3 }}>
                            <Button size="sm" outline color="info" className="rounded-pill" onClick={() => addBackup()}>
                                <Icon icon="restore-backup" margin="m-0" /> Add shard backup file
                            </Button>
                        </Col>
                    </Row>
                </>
            ) : (
                <div className="mt-2">
                    <Row>
                        <Col lg="3">
                            <label className="col-form-label">Directory Path</label>
                        </Col>
                        <Col>
                            <Input placeholder="Enter backup directory path"></Input>
                        </Col>
                    </Row>
                </div>
            )}
        </>
    );
}

function BackupSourceFragmentCloud() {
    return (
        <div className="mt-2">
            <Row>
                <Col lg="3">
                    <Label className="col-form-label" id="CloudBackupLinkInfo">
                        Backup Link <Icon icon="info" color="info" margin="m-0" />
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

function BackupSourceFragmentAws() {
    const [useCustomHost, setUseCustomHost] = useState(false);
    const toggleUseCustomHost = () => {
        setUseCustomHost(!useCustomHost);
    };
    return (
        <div className="mt-2">
            <Row>
                <Col lg={{ offset: 3 }}>
                    <Switch color="primary" selected={useCustomHost} toggleSelection={toggleUseCustomHost}>
                        Use a custom S3 host
                    </Switch>
                </Col>
            </Row>

            <Collapse isOpen={useCustomHost}>
                <Row>
                    <Col lg={{ offset: 3 }}>
                        <Switch color="primary" selected={null} toggleSelection={null}>
                            Force path style <Icon icon="info" color="info" id="CloudBackupLinkInfo" margin="m-0" />
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
                    <Col lg="3">
                        <Label className="col-form-label">Custom server URL</Label>
                    </Col>
                    <Col>
                        <Input />
                    </Col>
                </Row>
            </Collapse>

            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Secret key</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Aws Region</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Name</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Bucket Name</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col lg="3">
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

function BackupSourceFragmentAzure() {
    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Account Name</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Account Key</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Container</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col lg="3">
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

function BackupSourceFragmentGcp() {
    return (
        <div className="mt-2">
            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Bucket Name</Label>
                </Col>
                <Col>
                    <Input />
                </Col>
            </Row>

            <Row className="mt-2">
                <Col lg="3">
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
                <Col lg="3">
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

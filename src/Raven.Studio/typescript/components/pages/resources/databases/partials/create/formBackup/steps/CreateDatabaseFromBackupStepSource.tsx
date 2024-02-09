import { Switch } from "components/common/Checkbox";
import { FormSelect, FormSwitch, FormSelectCreatable } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { LicenseRestrictions } from "components/common/LicenseRestrictions";
import {
    OptionWithIcon,
    SingleValueWithIcon,
    SelectOptionWithIcon,
    SelectOption,
    InputNotHidden,
} from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React, { useState } from "react";
import { useFormContext, useWatch, useFieldArray, FieldPath } from "react-hook-form";
import { Collapse, Row, Col, Label, Input, UncontrolledPopover, PopoverBody, Button, Badge } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../createDatabaseFromBackupValidation";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import { InputActionMeta, OptionProps, components, GroupHeadingProps } from "react-select";
import { todo } from "common/developmentHelper";
import moment from "moment";
import generalUtils from "common/generalUtils";

const backupSourceImg = require("Content/img/createDatabase/backup-source.svg");

export default function CreateDatabaseFromBackupStepSource() {
    const { control } = useFormContext<FormData>();

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));

    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    const formValues = useWatch({
        control,
    });

    return (
        <>
            <Collapse isOpen={formValues.source == null}>
                <div className="d-flex justify-content-center">
                    <img src={backupSourceImg} alt="" className="step-img" />
                </div>
            </Collapse>
            <h2>Backup Source</h2>

            <Row className="mt-2">
                <Col lg="3">
                    <Label className="col-form-label">Backup Source</Label>
                </Col>
                <Col>
                    <FormSelect
                        control={control}
                        name="source"
                        options={sourceOptions}
                        isSearchable={false}
                        components={{
                            Option: OptionWithIcon,
                            SingleValue: SingleValueWithIcon,
                        }}
                    />
                </Col>
            </Row>
            <Collapse isOpen={formValues.source != null}>
                <>
                    {formValues.source.sourceType === "local" && <BackupSourceFragmentLocal />}
                    {formValues.source.sourceType === "cloud" && <BackupSourceFragmentCloud />}
                    {formValues.source.sourceType === "amazonS3" && <BackupSourceFragmentAmazonS3 />}
                    {formValues.source.sourceType === "azure" && <BackupSourceFragmentAzure />}
                    {formValues.source.sourceType === "googleCloud" && <BackupSourceFragmentGoogleCloud />}
                    {/* {formValues.isSharded && formValues.source !== "local" ? (
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
                    ) : (
                        <Row className="mt-2">
                            <Col lg="3">
                                <Label className="col-form-label">Restore Point</Label>
                            </Col>
                            <Col>
                                {formValues.source && (
                                    <>
                                        <RestorePointSelector />
                                    </>
                                )}
                            </Col>
                        </Row>
                    )} */}

                    <Row>
                        <Col lg={{ offset: 3, size: 9 }}>
                            <div className="mt-4">
                                <FormSwitch
                                    control={control}
                                    name="source.isDisableOngoingTasksAfterRestore"
                                    color="primary"
                                >
                                    <Icon icon="ongoing-tasks" addon="cancel" />
                                    Disable ongoing tasks after restore
                                </FormSwitch>
                                <FormSwitch control={control} name="source.isSkipIndexes" color="primary">
                                    <Icon icon="index" /> Skip indexes
                                </FormSwitch>
                                {/* TODO: Lock encryption when the source file is encrypted */}
                                {hasEncryption ? (
                                    <LicenseRestrictions
                                        isAvailable={true}
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
                                        <FormSwitch
                                            control={control}
                                            name="source.isEncrypted"
                                            color="primary"
                                            disabled={!isSecureServer}
                                        >
                                            <Icon icon="encryption" />
                                            Encrypt at Rest
                                        </FormSwitch>
                                    </LicenseRestrictions>
                                ) : (
                                    <LicenseRestrictions
                                        isAvailable={false}
                                        featureName={
                                            <strong className="text-primary">
                                                <Icon icon="storage" addon="encryption" margin="m-0" /> Storage
                                                encryption
                                            </strong>
                                        }
                                        className="d-inline-block"
                                    >
                                        <Switch color="primary" selected={false} toggleSelection={null} disabled={true}>
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

const sourceOptions: SelectOptionWithIcon[] = [
    {
        value: "local",
        label: "Local Server Directory",
        icon: "storage",
    },
    {
        value: "ravenCloud",
        label: "RavenDB Cloud",
        icon: "cloud",
    },
    {
        value: "aws",
        label: "Amazon S3",
        icon: "aws",
    },
    {
        value: "azure",
        label: "Microsoft Azure",
        icon: "azure",
    },
    {
        value: "gcp",
        label: "Google Cloud Platform",
        icon: "gcp",
    },
];

function BackupSourceFragmentLocal() {
    const { resourcesService } = useServices();
    const { control, setValue } = useFormContext<FormData>();

    const formValues = useWatch({
        control,
    });

    const directory = formValues.source.sourceData.local.directory;

    const { fields, append, remove } = useFieldArray({
        control,
        name: "source.sourceData.local.restorePoints",
    });

    // TODO debounce
    const asyncGetLocalFolderPathOptions = useAsync(async () => {
        const dto = await resourcesService.getFolderPathOptions_ServerLocal(directory, true);
        return dto.List.map((x) => ({ value: x, label: x }) satisfies SelectOption);
    }, [directory]);

    // TODO make autocomplete component?
    const onPathChange = (value: string, action: InputActionMeta) => {
        if (action?.action !== "input-blur" && action?.action !== "menu-close") {
            setValue("source.sourceData.local.directory", value);
        }
    };

    return (
        <Row>
            <Col lg="3">
                <label className="col-form-label">
                    Directory Path &<br />
                    Restore Point
                </label>
            </Col>
            <Col className="mt-2">
                <FormSelectCreatable
                    control={control}
                    name="source.sourceData.local.directory"
                    options={asyncGetLocalFolderPathOptions.result || []}
                    inputValue={directory ?? ""}
                    placeholder="Enter backup directory path"
                    onInputChange={onPathChange}
                    components={{ Input: InputNotHidden }}
                    tabSelectsValue
                    blurInputOnSelect={false}
                />
                {fields.map((field, index) => (
                    <SourceLocalRestorePoint key={field.id} index={index} remove={remove} />
                ))}
                {formValues.basicInfo.isSharded && (
                    <Button
                        size="sm"
                        color="shard"
                        className="rounded-pill mt-2"
                        onClick={() => append({ restorePoint: null, nodeTag: "" })}
                    >
                        <Icon icon="restore-backup" margin="m-0" /> Add shard restore point
                    </Button>
                )}
            </Col>
        </Row>
    );
}

const unknownDatabaseName = "Unknown Database";

interface RestorePoint {
    dateTime: string;
    location: string;
    fileName: string;
    isSnapshotRestore: boolean;
    isIncremental: boolean;
    isEncrypted: boolean;
    filesToRestore: number;
    databaseName: string;
    nodeTag: string;
    backupType: string;
}

function mapToRestorePoint(dto: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoint): RestorePoint {
    let backupType = "";
    if (dto.IsSnapshotRestore) {
        if (dto.IsIncremental) {
            backupType = "Incremental ";
        }
        backupType += "Snapshot";
    } else if (dto.IsIncremental) {
        backupType = "Incremental";
    } else {
        backupType = "Full";
    }

    return {
        dateTime: moment(dto.DateTime).format(generalUtils.dateFormat),
        location: dto.Location,
        fileName: dto.FileName,
        isSnapshotRestore: dto.IsSnapshotRestore,
        isIncremental: dto.IsIncremental,
        isEncrypted: dto.IsEncrypted,
        filesToRestore: dto.FilesToRestore,
        databaseName: dto.DatabaseName,
        nodeTag: dto.NodeTag || "-",
        backupType,
    };
}

interface GroupedOption {
    label: string;
    options: SelectOption<RestorePoint>[];
}

const RestorePointGroupHeading = (props: GroupHeadingProps<SelectOption<RestorePoint>>) => {
    return (
        <div>
            <small>
                <strong className="text-success ms-2">{props.data.label}</strong>
            </small>
            <Row className="d-flex align-items-center text-center p-1">
                <Col lg="4">
                    <small>DATE</small>
                </Col>
                <Col lg="2">
                    <small>BACKUP TYPE</small>
                </Col>
                <Col lg="2">
                    <small>ENCRYPTED</small>
                </Col>
                <Col lg="2">
                    <small>NODE TAG</small>
                </Col>
                <Col lg="2">
                    <small>FILES TO RESTORE</small>
                </Col>
            </Row>
        </div>
    );
};

export function RestorePointOption(props: OptionProps<SelectOption<RestorePoint>>) {
    const { data } = props;

    todo("Styling", "Kwiato", "backup type color");
    return (
        <div>
            <components.Option {...props}>
                <Row>
                    <Col lg="4" className="text-center">
                        <small>{data.value.dateTime}</small>
                    </Col>
                    <Col lg="2" className="text-center">
                        <small>{data.value.backupType}</small>
                    </Col>
                    <Col lg="2" className="text-center">
                        <small>
                            {data.value.isEncrypted ? <Icon icon="lock" color="success" /> : <Icon icon="unsecure" />}
                        </small>
                    </Col>
                    <Col lg="2" className="text-center">
                        <small>{data.value.nodeTag}</small>
                    </Col>
                    <Col lg="2" className="text-center">
                        <small>{data.value.filesToRestore}</small>
                    </Col>
                </Row>
            </components.Option>
        </div>
    );
}

function SourceLocalRestorePoint({ index, remove }: { index: number; remove: (index: number) => void }) {
    const { control } = useFormContext<FormData>();

    const formValues = useWatch({
        control,
    });

    const directory = formValues.source.sourceData.local.directory;
    const isSharded = formValues.basicInfo.isSharded;

    const nodeTagOptions: SelectOptionWithIcon[] = useAppSelector(clusterSelectors.allNodeTags).map((tag) => ({
        value: tag,
        label: tag,
        icon: "node",
        iconColor: "node",
    }));

    const { resourcesService } = useServices();

    // TODO debounce
    const asyncGetRestorePointsOptions = useAsync(async () => {
        const dto = await resourcesService.getRestorePoints_Local(directory, null, true, isSharded ? index : undefined);

        const groups: GroupedOption[] = [];

        dto.List.forEach((dtoRestorePoint) => {
            const databaseName = dtoRestorePoint.DatabaseName ?? unknownDatabaseName;

            if (!groups.find((x) => x.label === databaseName)) {
                groups.push({ label: databaseName, options: [] });
            }

            const group = groups.find((x) => x.label === databaseName);

            const restorePointValue = mapToRestorePoint(dtoRestorePoint);
            group.options.push({
                value: restorePointValue,
                label: `${restorePointValue.dateTime}, ${restorePointValue.backupType} Backup`,
            });
        });

        return groups;
    }, [directory]);

    const restorePointName: FieldPath<FormData> = `source.sourceData.local.restorePoints.${index}`;

    return (
        <>
            <Row>
                {formValues.basicInfo.isSharded && (
                    <>
                        <div className="d-flex justify-content-between">
                            <Badge pill color="info" className="px-1">
                                <Icon icon="shard" margin="m-0" />#{index}
                            </Badge>
                            {index > 0 && (
                                <Button
                                    size="sm"
                                    outline
                                    color="danger"
                                    className="rounded-pill"
                                    onClick={() => remove(index)}
                                >
                                    <Icon icon="trash" margin="m-0" />
                                </Button>
                            )}
                        </div>
                        <Col lg="3">
                            <FormSelect
                                control={control}
                                name={`${restorePointName}.nodeTag`}
                                options={nodeTagOptions}
                                placeholder={<Icon icon="node" color="node" />}
                                isSearchable={false}
                                components={{
                                    Option: OptionWithIcon,
                                    SingleValue: SingleValueWithIcon,
                                }}
                            />
                        </Col>
                    </>
                )}
            </Row>
            <FormSelect
                control={control}
                name={`${restorePointName}.restorePoint`}
                placeholder="Select restore point"
                options={asyncGetRestorePointsOptions.result || []}
                components={{
                    GroupHeading: RestorePointGroupHeading,
                    Option: RestorePointOption,
                }}
            />
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

function BackupSourceFragmentAmazonS3() {
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

function BackupSourceFragmentGoogleCloud() {
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

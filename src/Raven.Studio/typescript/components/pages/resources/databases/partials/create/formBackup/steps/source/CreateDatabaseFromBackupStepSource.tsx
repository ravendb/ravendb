import { Switch } from "components/common/Checkbox";
import { FormSelect, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { LicenseRestrictions } from "components/common/LicenseRestrictions";
import { OptionWithIcon, SingleValueWithIcon, SelectOptionWithIcon } from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Collapse, Row, Col, Label } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import BackupSourceLocal from "./BackupSourceLocal";
import BackupSourceAmazonS3 from "./BackupSourceAmazonS3";
import BackupSourceCloud from "./BackupSourceCloud";
import BackupSourceAzure from "./BackupSourceAzure";
import BackupSourceGoogleCloud from "components/pages/resources/databases/partials/create/formBackup/steps/source/BackupSourceGoogleCloud";

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
            <Collapse isOpen={formValues.source.sourceType === null}>
                <div className="d-flex justify-content-center">
                    <img src={backupSourceImg} alt="Backup source" className="step-img" />
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
                        name="source.sourceType"
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
                {formValues.source.sourceType === "local" && <BackupSourceLocal />}
                {formValues.source.sourceType === "cloud" && <BackupSourceCloud />}
                {formValues.source.sourceType === "amazonS3" && <BackupSourceAmazonS3 />}
                {formValues.source.sourceType === "azure" && <BackupSourceAzure />}
                {formValues.source.sourceType === "googleCloud" && <BackupSourceGoogleCloud />}

                <FormSwitch
                    className="mt-4"
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
                {/* TODO: to component */}
                {hasEncryption ? (
                    <LicenseRestrictions
                        isAvailable={true}
                        message={
                            <>
                                <p className="lead text-warning">
                                    <Icon icon="unsecure" margin="m-0" /> Authentication is off
                                </p>
                                <p>
                                    <strong>Encription at Rest</strong> is only possible when authentication is enabled
                                    and a server certificate has been defined.
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
                                <Icon icon="storage" addon="encryption" margin="m-0" /> Storage encryption
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
            </Collapse>
        </>
    );
}

const sourceOptions: SelectOptionWithIcon<restoreSource>[] = [
    {
        value: "local",
        label: "Local Server Directory",
        icon: "storage",
    },
    {
        value: "cloud",
        label: "RavenDB Cloud",
        icon: "cloud",
    },
    {
        value: "amazonS3",
        label: "Amazon S3",
        icon: "aws",
    },
    {
        value: "azure",
        label: "Microsoft Azure",
        icon: "azure",
    },
    {
        value: "googleCloud",
        label: "Google Cloud Platform",
        icon: "gcp",
    },
];

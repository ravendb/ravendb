import { FormSelect, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { OptionWithIcon, SingleValueWithIcon, SelectOptionWithIcon } from "components/common/select/Select";
import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Collapse, Row, Col, Label } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import BackupSourceLocal from "./BackupSourceLocal";
import BackupSourceAmazonS3 from "./BackupSourceAmazonS3";
import BackupSourceCloud from "./BackupSourceCloud";
import BackupSourceAzure from "./BackupSourceAzure";
import BackupSourceGoogleCloud from "components/pages/resources/databases/partials/create/formBackup/steps/source/BackupSourceGoogleCloud";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import AuthenticationOffMessage from "components/pages/resources/databases/partials/create/shared/AuthenticationOffMessage";
import EncryptionUnavailableMessage from "components/pages/resources/databases/partials/create/shared/EncryptionUnavailableMessage";
import { useAppSelector } from "components/store";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";

const backupSourceImg = require("Content/img/createDatabase/backup-source.svg");

export default function CreateDatabaseFromBackupStepSource() {
    const { control } = useFormContext<FormData>();
    const {
        sourceStep: { sourceType },
    } = useWatch({
        control,
    });

    return (
        <>
            <Collapse isOpen={sourceType == null}>
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
                        name="sourceStep.sourceType"
                        options={sourceOptions}
                        isSearchable={false}
                        components={{
                            Option: OptionWithIcon,
                            SingleValue: SingleValueWithIcon,
                        }}
                    />
                </Col>
            </Row>
            <Collapse isOpen={sourceType != null}>
                {sourceType === "local" && <BackupSourceLocal />}
                {sourceType === "cloud" && <BackupSourceCloud />}
                {sourceType === "amazonS3" && <BackupSourceAmazonS3 />}
                {sourceType === "azure" && <BackupSourceAzure />}
                {sourceType === "googleCloud" && <BackupSourceGoogleCloud />}

                <FormSwitch
                    className="mt-4"
                    control={control}
                    name="sourceStep.isDisableOngoingTasksAfterRestore"
                    color="primary"
                >
                    <Icon icon="ongoing-tasks" addon="cancel" />
                    Disable ongoing tasks after restore
                </FormSwitch>
                <FormSwitch control={control} name="sourceStep.isSkipIndexes" color="primary">
                    <Icon icon="index" /> Skip indexes
                </FormSwitch>
                <IsEncryptedField />
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

function IsEncryptedField() {
    const { control, formState } = useFormContext<FormData>();
    const {
        sourceStep: { sourceType, sourceData },
    } = useWatch({
        control,
    });

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    const isRestorePointEncrypted = sourceType
        ? sourceData[sourceType].pointsWithTags[0].restorePoint?.isEncrypted
        : false;

    const isEncryptionDisabled = !hasEncryption || !isSecureServer || isRestorePointEncrypted || formState.isSubmitting;

    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive: !hasEncryption,
                    message: <EncryptionUnavailableMessage />,
                },
                {
                    isActive: !isSecureServer,
                    message: <AuthenticationOffMessage />,
                },
                {
                    isActive: isRestorePointEncrypted,
                    message: "Fill Backup Encryption Key above",
                },
            ]}
            popoverPlacement="left"
        >
            <FormSwitch color="primary" control={control} name="sourceStep.isEncrypted" disabled={isEncryptionDisabled}>
                <Icon icon="encryption" />
                Encrypt at Rest
                {!hasEncryption && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
            </FormSwitch>
        </ConditionalPopover>
    );
}

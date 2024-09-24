import { FormSelect, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { OptionWithIcon, SingleValueWithIcon, SelectOptionWithIcon } from "components/common/select/Select";
import React, { useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Collapse, Row, Col, Label } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData, RestoreSource } from "../../createDatabaseFromBackupValidation";
import BackupSourceLocal from "./BackupSourceLocal";
import BackupSourceAmazonS3 from "./BackupSourceAmazonS3";
import BackupSourceRavenCloud from "./BackupSourceRavenCloud";
import BackupSourceAzure from "./BackupSourceAzure";
import BackupSourceGoogleCloud from "components/pages/resources/databases/partials/create/formBackup/steps/source/BackupSourceGoogleCloud";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import AuthenticationOffMessage from "components/pages/resources/databases/partials/create/shared/AuthenticationOffMessage";
import EncryptionUnavailableMessage from "components/pages/resources/databases/partials/create/shared/EncryptionUnavailableMessage";

const backupSourceImg = require("Content/img/createDatabase/backup-source.svg");

export default function CreateDatabaseFromBackupStepSource() {
    const { control, setValue } = useFormContext<FormData>();
    const {
        sourceStep: { sourceData, sourceType },
    } = useWatch({
        control,
    });

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    const firstRestorePoint = sourceData[sourceType]?.pointsWithTags?.[0]?.restorePoint;
    const isRestorePointEncrypted = firstRestorePoint?.isEncrypted ?? false;
    const isRestorePointSnapshot = firstRestorePoint?.isSnapshotRestore ?? false;

    useEffect(() => {
        const canBeEncryptedOnServer = hasEncryption && isSecureServer;
        const isEncrypted = canBeEncryptedOnServer && isRestorePointEncrypted && !isRestorePointSnapshot;

        setValue(`sourceStep.isEncrypted`, isEncrypted);
    }, [isSecureServer, hasEncryption, isRestorePointEncrypted, isRestorePointSnapshot, setValue]);

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
                {sourceType === "ravenCloud" && <BackupSourceRavenCloud />}
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
                    <Icon icon="index" />
                    Skip indexes
                </FormSwitch>
                <IsEncryptedField
                    isRestorePointSnapshot={isRestorePointSnapshot}
                    isRestorePointEncrypted={isRestorePointEncrypted}
                />
            </Collapse>
        </>
    );
}

const sourceOptions: SelectOptionWithIcon<RestoreSource>[] = [
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

interface IsEncryptedFieldProps {
    isRestorePointSnapshot: boolean;
    isRestorePointEncrypted: boolean;
}

function IsEncryptedField({ isRestorePointSnapshot, isRestorePointEncrypted }: IsEncryptedFieldProps) {
    const { control, formState } = useFormContext<FormData>();

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    const isEncryptionDisabled = !hasEncryption || !isSecureServer || formState.isSubmitting || isRestorePointSnapshot;

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
                    isActive: isRestorePointSnapshot,
                    message: (
                        <span>
                            {isRestorePointEncrypted
                                ? "To restore an encrypted snapshot, you only need to enter the key in the 'Backup Encryption Key' field above"
                                : "Snapshot is not encrypted"}
                        </span>
                    ),
                },
            ]}
            popoverPlacement="top"
        >
            <FormSwitch color="primary" control={control} name="sourceStep.isEncrypted" disabled={isEncryptionDisabled}>
                <Icon icon="encryption" />
                Encrypt at Rest
                {!hasEncryption && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
            </FormSwitch>
        </ConditionalPopover>
    );
}

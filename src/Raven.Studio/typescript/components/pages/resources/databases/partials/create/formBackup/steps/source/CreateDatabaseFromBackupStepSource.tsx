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
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import AuthenticationOffMessage from "components/pages/resources/databases/partials/create/shared/AuthenticationOffMessage";
import EncryptionUnavailableMessage from "components/pages/resources/databases/partials/create/shared/EncryptionUnavailableMessage";
import { useAppSelector } from "components/store";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";

const backupSourceImg = require("Content/img/createDatabase/backup-source.svg");

export default function CreateDatabaseFromBackupStepSource() {
    const { control, setValue } = useFormContext<FormData>();
    const {
        sourceStep: { sourceData, sourceType },
    } = useWatch({
        control,
    });

    const firstRestorePoint = sourceData[sourceType]?.pointsWithTags?.[0]?.restorePoint;
    const isFirstRestorePointEncrypted = firstRestorePoint?.isEncrypted ?? false;
    const isFirstRestorePointSnapshot = firstRestorePoint?.isSnapshotRestore ?? false;

    const encryptionKeyFromSourceStep = sourceData[sourceType]?.encryptionKey ?? "";

    // Toggle encryption step based on the first restore point encryption
    useEffect(() => {
        setValue(`sourceStep.isEncrypted`, isFirstRestorePointEncrypted);
    }, [isFirstRestorePointEncrypted, setValue]);

    // Set encryption key in encryption step if the first restore point is encrypted snapshot
    useEffect(() => {
        if (isFirstRestorePointSnapshot && isFirstRestorePointEncrypted) {
            setValue(`encryptionStep.key`, encryptionKeyFromSourceStep);
        }
    }, [isFirstRestorePointSnapshot, isFirstRestorePointEncrypted, encryptionKeyFromSourceStep, setValue]);

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
                <IsEncryptedField isFirstRestorePointSnapshot={isFirstRestorePointSnapshot} />
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
    isFirstRestorePointSnapshot: boolean;
}

function IsEncryptedField({ isFirstRestorePointSnapshot }: IsEncryptedFieldProps) {
    const { control, formState } = useFormContext<FormData>();

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    const isEncryptionDisabled =
        !hasEncryption || !isSecureServer || isFirstRestorePointSnapshot || formState.isSubmitting;

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

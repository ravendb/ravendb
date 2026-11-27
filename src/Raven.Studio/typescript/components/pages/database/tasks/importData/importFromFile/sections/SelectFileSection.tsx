import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Badge from "react-bootstrap/Badge";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import FileDropzone from "components/common/FileDropzone";
import ImportSection from "components/pages/database/tasks/importData/shared/ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import {
    RestrictedImportItem,
    useImportRestrictions,
} from "components/pages/database/tasks/importData/shared/useImportRestrictions";
import { useRavenLink } from "components/hooks/useRavenLink";
import genUtils from "common/generalUtils";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import MinPeriodWarningAlert from "components/pages/database/tasks/importData/importFromFile/sections/MinPeriodWarningAlert";

const backupExtensions = [
    "ravendb-full-backup",
    "ravendb-encrypted-full-backup",
    "ravendb-incremental-backup",
    "ravendb-encrypted-incremental-backup",
];

export default function SelectFileSection() {
    const t = useStudioTranslation("importData");
    const { control, setValue, formState } = useFormContext<ImportFromFileFormData>();
    const file = useWatch({ control, name: "file" });
    const { allRestrictedItems } = useImportRestrictions();

    const licenseRestrictedItems = allRestrictedItems.filter((x) => x.reason === "license");
    const otherRestrictedItems = allRestrictedItems.filter((x) => x.reason !== "license");

    const fileExtension = file ? genUtils.getFileExtension(file.name) : null;
    const isBackupFile = fileExtension ? backupExtensions.includes(fileExtension) : false;
    const fileError = formState.errors.file?.message;

    return (
        <ImportSection id="select-file" title={t("sections.selectFile")} errorPaths={["file"]}>
            <div className="card p-4">
                <FileDropzone
                    onChange={(files) =>
                        setValue("file", files[0] ?? null, { shouldValidate: true, shouldDirty: true })
                    }
                    maxFiles={1}
                    validExtensions={[
                        "ravendbdump",
                        ...backupExtensions,
                        "ravendb-snapshot",
                        "ravendb-encrypted-snapshot",
                    ]}
                    displayedExtensions={["ravendbdump"]}
                />
                {fileError && <div className="text-danger mt-2">{fileError}</div>}
                {isBackupFile && <BackupFileAlert />}
                <LicenseRestrictionsAlert items={licenseRestrictedItems} />
                <MinPeriodWarningAlert />
                <OtherRestrictionsAlert items={otherRestrictedItems} />
            </div>
        </ImportSection>
    );
}

function BackupFileAlert() {
    return (
        <RichAlert variant="warning" className="mt-3 mb-0">
            <StudioTrans ns="importData" i18nKey="importFile.backupFile" components={{ strong: <strong /> }} />
        </RichAlert>
    );
}

interface RestrictionsAlertProps {
    items: RestrictedImportItem[];
}

function LicenseRestrictionsAlert({ items }: RestrictionsAlertProps) {
    const t = useStudioTranslation("importData");
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    if (items.length === 0) {
        return null;
    }

    return (
        <RichAlert
            variant="warning"
            title={t("importFile.licenseRestrictions.title")}
            titleTag="h4"
            className="mt-3 mb-0"
        >
            <div>{t("importFile.licenseRestrictions.description")}</div>
            <div className="d-flex gap-2 flex-wrap mt-2">
                {items.map(({ key, tooltip, icon, label }) => (
                    <Badge key={key} bg="secondary" title={tooltip}>
                        <Icon icon={icon} addon="license" /> {label}
                    </Badge>
                ))}
            </div>
            <div className="mt-2">
                <Icon icon="info" color="info" /> {t("importFile.licenseRestrictions.upgrade")}{" "}
                <a href={buyLink} target="_blank" rel="noreferrer">
                    {t("importFile.licenseRestrictions.licenseComparison")} <Icon icon="newtab" margin="m-0" />
                </a>
            </div>
        </RichAlert>
    );
}

function OtherRestrictionsAlert({ items }: RestrictionsAlertProps) {
    const t = useStudioTranslation("importData");

    if (items.length === 0) {
        return null;
    }

    return (
        <RichAlert
            variant="warning"
            title={t("importFile.otherRestrictions.title")}
            titleTag="h4"
            className="mt-3 mb-0"
        >
            <div>{t("importFile.otherRestrictions.description")}</div>
            <div className="d-flex gap-2 flex-wrap mt-2">
                {items.map(({ key, icon, reason, tooltip, label }) => (
                    <Badge key={key} bg="secondary" title={tooltip}>
                        <Icon icon={icon} addon={reason === "sharding" ? "sharding" : "certificate"} /> {label}
                    </Badge>
                ))}
            </div>
        </RichAlert>
    );
}

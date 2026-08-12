import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Badge from "react-bootstrap/Badge";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import FileDropzone from "components/common/FileDropzone";
import ImportSection from "./ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import { RestrictedImportItem, useImportRestrictions } from "../useImportRestrictions";
import { useRavenLink } from "components/hooks/useRavenLink";
import genUtils from "common/generalUtils";
import MinPeriodWarningAlert from "components/pages/database/tasks/importData/importFromFile/sections/MinPeriodWarningAlert";

const backupExtensions = [
    "ravendb-full-backup",
    "ravendb-encrypted-full-backup",
    "ravendb-incremental-backup",
    "ravendb-encrypted-incremental-backup",
];

export default function SelectFileSection() {
    const { control, setValue, formState } = useFormContext<ImportFromFileFormData>();
    const file = useWatch({ control, name: "file" });
    const { allRestrictedItems } = useImportRestrictions();

    const licenseRestrictedItems = allRestrictedItems.filter((x) => x.reason === "license");
    const otherRestrictedItems = allRestrictedItems.filter((x) => x.reason !== "license");

    const fileExtension = file ? genUtils.getFileExtension(file.name) : null;
    const isBackupFile = fileExtension ? backupExtensions.includes(fileExtension) : false;
    const fileError = formState.errors.file?.message;

    return (
        <ImportSection id="select-file" title="Select file to import" errorPaths={["file"]}>
            <div className="card p-4">
                <div className="small-label mb-1">Select file</div>
                <FileDropzone
                    onChange={(files) =>
                        setValue("file", files[0] ?? null, { shouldValidate: true, shouldDirty: true })
                    }
                    maxFiles={1}
                    // backup/snapshot extensions are selectable on purpose - dedicated alerts below
                    // redirect the user to the Restore flow instead of a generic rejection
                    validExtensions={[
                        "ravendbdump",
                        ...backupExtensions,
                        "ravendb-snapshot",
                        "ravendb-encrypted-snapshot",
                    ]}
                    // only .ravendbdump is actually importable - the rest are accepted so the
                    // alerts below can redirect the user to the Restore flow
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
            The selected file is a <strong>RavenDB Backup file</strong>. Please use the <strong>Restore</strong> option
            (under Create New Database) in order to restore data from a RavenDB Backup file.
        </RichAlert>
    );
}

interface RestrictionsAlertProps {
    items: RestrictedImportItem[];
}

function LicenseRestrictionsAlert({ items }: RestrictionsAlertProps) {
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    if (items.length === 0) {
        return null;
    }

    return (
        <RichAlert variant="warning" title="Some data may not be imported" className="mt-3 mb-0">
            <div>
                Your license doesn&apos;t include the following features. Any related data in this file will be skipped
                automatically.
            </div>
            <div className="d-flex gap-2 flex-wrap mt-2">
                {items.map((item) => (
                    <Badge key={item.key} bg="secondary">
                        <Icon icon="license" /> {item.label}
                    </Badge>
                ))}
            </div>
            <div className="mt-2">
                <Icon icon="info" color="info" /> Upgrade to include this data on import.{" "}
                <a href={buyLink} target="_blank" rel="noreferrer">
                    See license comparison <Icon icon="newtab" margin="m-0" />
                </a>
            </div>
        </RichAlert>
    );
}

function OtherRestrictionsAlert({ items }: RestrictionsAlertProps) {
    if (items.length === 0) {
        return null;
    }

    return (
        <RichAlert variant="warning" title="Some data cannot be imported" className="mt-3 mb-0">
            <div>
                The following features are unavailable for this database or for your certificate. Any related data in
                this file will be skipped automatically.
            </div>
            <div className="d-flex gap-2 flex-wrap mt-2">
                {items.map((item) => (
                    <Badge key={item.key} bg="secondary" title={item.tooltip}>
                        <Icon icon={item.reason === "sharding" ? "sharding" : "certificate"} /> {item.label}
                    </Badge>
                ))}
            </div>
        </RichAlert>
    );
}

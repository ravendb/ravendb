import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Badge from "react-bootstrap/Badge";
import { Icon } from "components/common/Icon";
import FileDropzone from "components/common/FileDropzone";
import ImportSection from "./ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import { RestrictedImportFeature } from "../useImportLicenseRestrictions";
import { useRavenLink } from "components/hooks/useRavenLink";
import genUtils from "common/generalUtils";

const backupExtensions = [
    "ravendb-full-backup",
    "ravendb-encrypted-full-backup",
    "ravendb-incremental-backup",
    "ravendb-encrypted-incremental-backup",
];

interface SelectFileSectionProps {
    restrictedFeatures: RestrictedImportFeature[];
}

export default function SelectFileSection({ restrictedFeatures }: SelectFileSectionProps) {
    const { control, setValue, formState } = useFormContext<ImportFromFileFormData>();
    const file = useWatch({ control, name: "file" });
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    const fileExtension = file ? genUtils.getFileExtension(file.name) : null;
    const isBackupFile = fileExtension ? backupExtensions.includes(fileExtension) : false;
    const fileError = formState.errors.file?.message;

    return (
        <ImportSection id="select-file" title="Select file to import">
            <div className="card p-4">
                <div className="small-label mb-1">Select file</div>
                <FileDropzone
                    onChange={(files) => setValue("file", files[0] ?? null, { shouldValidate: true, shouldDirty: true })}
                    maxFiles={1}
                    validExtensions={["ravendbdump", ...backupExtensions, "ravendb-snapshot", "ravendb-encrypted-snapshot"]}
                />
                {file && (
                    <div className="mt-2">
                        <Icon icon="document" /> {file.name}
                    </div>
                )}
                {fileError && <div className="text-danger mt-2">{fileError}</div>}
                {isBackupFile && (
                    <Alert variant="warning" className="mt-3 mb-0">
                        The selected file is a <strong>RavenDB Backup file</strong>. Please use the{" "}
                        <strong>Restore</strong> option (under Create New Database) in order to restore data from a
                        RavenDB Backup file.
                    </Alert>
                )}
                {restrictedFeatures.length > 0 ? (
                    <Alert variant="warning" className="mt-3 mb-0">
                        <div className="fw-bold">
                            <Icon icon="warning" /> Some data may not be imported
                        </div>
                        <div>
                            Your license doesn&apos;t include the following features. Any related data in this file
                            will be skipped automatically.
                        </div>
                        <div className="d-flex gap-2 flex-wrap mt-2">
                            {restrictedFeatures.map((feature) => (
                                <Badge key={feature.settingKey} bg="secondary">
                                    <Icon icon="license" /> {feature.label}
                                </Badge>
                            ))}
                        </div>
                        <div className="mt-2">
                            <Icon icon="info" /> Upgrade to include this data on import.{" "}
                            <a href={buyLink} target="_blank" rel="noreferrer">
                                See license comparison <Icon icon="newtab" margin="m-0" />
                            </a>
                        </div>
                    </Alert>
                ) : (
                    <Alert variant="info" className="mt-3 mb-0">
                        <Icon icon="info" /> Your import might contain settings that aren&apos;t available on your
                        current license. Those features will be turned off and disabled from your import.
                    </Alert>
                )}
            </div>
        </ImportSection>
    );
}

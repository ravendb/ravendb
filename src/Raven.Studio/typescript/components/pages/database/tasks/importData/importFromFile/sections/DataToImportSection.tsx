import React, { useEffect, useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Spinner from "react-bootstrap/Spinner";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import { getItemsToWarnAbout } from "../importFromFileUtils";
import { useDumpFileCollections } from "../useDumpFileCollections";

export default function DataToImportSection() {
    const { control, setValue } = useFormContext<ImportFromFileFormData>();
    const documents = useWatch({ control, name: "documents" });
    const file = useWatch({ control, name: "file" });
    const [collectionFilter, setCollectionFilter] = useState("");

    const isImportAll = useWatch({ control, name: "collections.isImportAllCollections" });
    const includedCollections = useWatch({ control, name: "collections.includedCollections" }) ?? [];

    // The Collections filter applies to collections in the imported FILE - the list is read
    // client-side from the selected dump. All collections start included; toggles exclude.
    const { collections: fileCollections, isReading, readError } = useDumpFileCollections(file ?? null);

    useEffect(() => {
        setValue("collections.includedCollections", fileCollections, { shouldDirty: true });
    }, [fileCollections, setValue]);

    const filteredCollections = fileCollections.filter((name) =>
        name.toLowerCase().includes(collectionFilter.toLowerCase())
    );

    const toggleCollection = (name: string, include: boolean) => {
        setValue(
            "collections.includedCollections",
            include ? [...includedCollections, name] : includedCollections.filter((x) => x !== name),
            { shouldDirty: true }
        );
    };

    const itemsToWarnAbout = getItemsToWarnAbout({ documents });

    const forceDocumentsOn = (value: boolean) => {
        if (value) {
            setValue("documents.isIncludeDocuments", true, { shouldDirty: true });
        }
    };

    const selectAllDocuments = () => {
        setValue("documents.isIncludeDocuments", true, { shouldDirty: true });
        setValue("documents.isIncludeAttachments", true, { shouldDirty: true });
        setValue("documents.isIncludeCounters", true, { shouldDirty: true });
        setValue("documents.isIncludeRevisions", true, { shouldDirty: true });
        setValue("documents.isIncludeTimeSeries", true, { shouldDirty: true });
        setValue("documents.isIncludeTimeSeriesDeletedRanges", true, { shouldDirty: true });
        setValue("documents.isIncludeArtificialDocuments", true, { shouldDirty: true });
        setValue("documents.isIncludeArchivedDocuments", true, { shouldDirty: true });
        setValue("documents.isIncludeExpiredDocuments", true, { shouldDirty: true });
        setValue("documents.isIncludeConflicts", true, { shouldDirty: true });
        setValue("documents.isIncludeCompareExchange", true, { shouldDirty: true });
        setValue("documents.isIncludeLegacyAttachments", true, { shouldDirty: true });
        setValue("documents.isIncludeDocumentsTombstones", true, { shouldDirty: true });
        setValue("documents.isIncludeCompareExchangeTombstones", true, { shouldDirty: true });
        setValue("documents.isIncludeSubscriptions", true, { shouldDirty: true });
    };

    return (
        <ImportSection id="data-to-import" title="Data to import">
            <div className="small-label mb-2">Choose collections to import</div>
            <div className="d-flex gap-3 mb-4">
                <Button
                    variant={isImportAll ? "primary" : "outline-secondary"}
                    className="flex-grow-1 py-3"
                    onClick={() => setValue("collections.isImportAllCollections", true, { shouldDirty: true })}
                >
                    <Icon icon="documents" /> Import all collections
                </Button>
                <Button
                    variant={!isImportAll ? "primary" : "outline-secondary"}
                    className="flex-grow-1 py-3"
                    onClick={() => setValue("collections.isImportAllCollections", false, { shouldDirty: true })}
                >
                    <Icon icon="document-group" /> Customize imported collections
                </Button>
            </div>
            {!isImportAll && (
                <div className="mb-4">
                    <Form.Control
                        type="text"
                        placeholder="Search for collection"
                        value={collectionFilter}
                        onChange={(e) => setCollectionFilter(e.target.value)}
                        className="mb-2"
                    />
                    <Table className="mb-0">
                        <thead>
                            <tr>
                                <th>Collection name</th>
                                <th className="text-end">
                                    Select all{" "}
                                    <Form.Check
                                        inline
                                        type="switch"
                                        aria-label="Select all collections"
                                        checked={
                                            filteredCollections.length > 0 &&
                                            filteredCollections.every((name) => includedCollections.includes(name))
                                        }
                                        onChange={(e) =>
                                            setValue(
                                                "collections.includedCollections",
                                                e.target.checked
                                                    ? [
                                                          ...includedCollections,
                                                          ...filteredCollections.filter(
                                                              (name) => !includedCollections.includes(name)
                                                          ),
                                                      ]
                                                    : includedCollections.filter(
                                                          (name) => !filteredCollections.includes(name)
                                                      ),
                                                { shouldDirty: true }
                                            )
                                        }
                                    />
                                </th>
                            </tr>
                        </thead>
                        <tbody>
                            {isReading && (
                                <tr>
                                    <td colSpan={2} className="text-muted">
                                        <Spinner size="sm" /> Reading collections from the selected file...
                                    </td>
                                </tr>
                            )}
                            {!isReading && readError && (
                                <tr>
                                    <td colSpan={2} className="text-warning">
                                        <Icon icon="warning" /> {readError}
                                    </td>
                                </tr>
                            )}
                            {!isReading && !readError && filteredCollections.length === 0 && (
                                <tr>
                                    <td colSpan={2} className="text-muted">
                                        {fileCollections.length === 0
                                            ? "No collections were found in the selected file."
                                            : "No collections match your filter."}
                                    </td>
                                </tr>
                            )}
                            {filteredCollections.map((name) => (
                                <tr key={name}>
                                    <td colSpan={2}>
                                        <Form.Check
                                            type="switch"
                                            label={name}
                                            checked={includedCollections.includes(name)}
                                            onChange={(e) => toggleCollection(name, e.target.checked)}
                                        />
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </Table>
                </div>
            )}

            <div className="d-flex justify-content-between align-items-center mb-2">
                <div className="small-label">Select documents and extensions</div>
                <Button variant="link" size="sm" onClick={selectAllDocuments}>
                    Select all
                </Button>
            </div>
            <div className="card p-4">
                <FormSwitch control={control} name="documents.isIncludeDocuments">
                    Include Documents
                </FormSwitch>
                <div className="ms-4">
                    <FormSwitch control={control} name="documents.isIncludeAttachments" afterChange={forceDocumentsOn}>
                        Include Attachments
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeCounters" afterChange={forceDocumentsOn}>
                        Include Counters
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeRevisions" afterChange={forceDocumentsOn}>
                        Include Revisions
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeTimeSeries" afterChange={forceDocumentsOn}>
                        Include Time Series
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeTimeSeriesDeletedRanges">
                        Include Time Series Deleted Ranges
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeLegacyAttachments">
                        Include Legacy Attachments
                    </FormSwitch>
                    <FormSwitch control={control} name="documents.isIncludeDocumentsTombstones">
                        Include Documents Tombstones
                    </FormSwitch>
                </div>
                <hr />
                <FormSwitch control={control} name="documents.isIncludeArtificialDocuments">
                    Include Artificial Documents{" "}
                    <PopoverWithHoverWrapper
                        message="Importing artificial documents might cause import error of Map-Reduce indexes with OutputReduceToCollection."
                    >
                        {/* prevent the click on the icon from toggling the surrounding switch label */}
                        <span onClick={(e) => e.preventDefault()}>
                            <Icon icon="info" margin="ms-1" />
                        </span>
                    </PopoverWithHoverWrapper>
                </FormSwitch>
                <FormSwitch control={control} name="documents.isIncludeArchivedDocuments">
                    Include Archived Documents
                </FormSwitch>
                <FormSwitch control={control} name="documents.isIncludeExpiredDocuments">
                    Include Expired Documents
                </FormSwitch>
                <FormSwitch control={control} name="documents.isIncludeConflicts">
                    Include Conflicts
                </FormSwitch>
                <FormSwitch control={control} name="documents.isIncludeCompareExchange">
                    Include Compare Exchange
                </FormSwitch>
                <FormSwitch control={control} name="documents.isIncludeCompareExchangeTombstones">
                    Include Compare Exchange Tombstones
                </FormSwitch>
                <FormSwitch control={control} name="documents.isIncludeSubscriptions">
                    Include Subscriptions
                </FormSwitch>

                {itemsToWarnAbout.length > 0 && (
                    <Alert variant="warning" className="mt-3 mb-0">
                        <Icon icon="warning" /> You are importing {itemsToWarnAbout.join(", ")} data without including
                        the Documents. The documents will be needed when importing this exported file to another
                        database.
                    </Alert>
                )}
            </div>
        </ImportSection>
    );
}

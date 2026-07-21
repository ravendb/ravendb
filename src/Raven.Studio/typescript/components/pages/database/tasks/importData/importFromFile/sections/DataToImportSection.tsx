import React, { useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import { ImportFromFileFormData } from "../importFromFileValidation";
import { getItemsToWarnAbout } from "../importFromFileUtils";
import { useImportLicenseRestrictions } from "../useImportLicenseRestrictions";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import Card from "react-bootstrap/Card";
import SelectCreatable from "components/common/select/SelectCreatable";

interface CollectionOption {
    label: string;
    value: string;
}

export default function DataToImportSection() {
    const { control, setValue } = useFormContext<ImportFromFileFormData>();
    const documents = useWatch({ control, name: "documents" });
    const { isDocumentToggleRestricted, getDocumentToggleRestrictionTooltip, getDocumentToggleLicenseRequired } =
        useImportLicenseRestrictions();
    const [collectionFilter, setCollectionFilter] = useState("");

    const isImportAll = useWatch({ control, name: "collections.isImportAllCollections" });
    const includedCollections = useWatch({ control, name: "collections.includedCollections" }) ?? [];

    // The list holds collections from the imported FILE, which the server cannot list before
    // the upload - so it starts empty and every entry is typed in by the user. Rows live in
    // local state so deselecting only turns the toggle off - the trash icon removes the row.
    const [manualCollections, setManualCollections] = useState<string[]>([]);

    const filteredCollections = manualCollections.filter((name) =>
        name.toLowerCase().includes(collectionFilter.toLowerCase())
    );

    const areAllFilteredCollectionsSelected =
        filteredCollections.length > 0 && filteredCollections.every((name) => includedCollections.includes(name));

    // typing filters the rows below; anything not on the list yet becomes an "Add ..." create option
    const isNewCollection = (input: string) => {
        const trimmed = input.trim();
        return trimmed.length > 0 && !manualCollections.some((name) => name.toLowerCase() === trimmed.toLowerCase());
    };

    const onCollectionPicked = (option: CollectionOption | null) => {
        if (!option) {
            return;
        }
        const trimmed = option.value.trim();
        if (isNewCollection(trimmed)) {
            setManualCollections((prev) => [...prev, trimmed]);
            setValue("collections.includedCollections", [...includedCollections, trimmed], { shouldDirty: true });
        }
        setCollectionFilter("");
    };

    const removeCollection = (name: string) => {
        setManualCollections((prev) => prev.filter((x) => x !== name));
        setValue(
            "collections.includedCollections",
            includedCollections.filter((x) => x !== name),
            { shouldDirty: true }
        );
    };

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

    const documentToggleNames = [
        "isIncludeDocuments",
        "isIncludeAttachments",
        "isIncludeCounters",
        "isIncludeRevisions",
        "isIncludeTimeSeries",
        "isIncludeTimeSeriesDeletedRanges",
        "isIncludeArtificialDocuments",
        "isIncludeArchivedDocuments",
        "isIncludeExpiredDocuments",
        "isIncludeConflicts",
        "isIncludeCompareExchange",
        "isIncludeLegacyAttachments",
        "isIncludeDocumentsTombstones",
        "isIncludeCompareExchangeTombstones",
        "isIncludeSubscriptions",
    ] as const;

    const selectableDocumentToggleNames = documentToggleNames.filter(
        (name) => name !== "isIncludeArchivedDocuments" || !isDocumentToggleRestricted("isIncludeArchivedDocuments")
    );

    const areAllDocumentsSelected = selectableDocumentToggleNames.every((name) => documents[name]);

    const setAllDocuments = (value: boolean) => {
        selectableDocumentToggleNames.forEach((name) => setValue(`documents.${name}`, value, { shouldDirty: true }));
    };

    const isArchivedRestricted = isDocumentToggleRestricted("isIncludeArchivedDocuments");

    return (
        <ImportSection id="data-to-import" title="Data to import">
            <div id="collections-to-import" className="small-label mb-2">
                Choose collections to import
            </div>
            <Card className="mb-4 p-4">
                <div className="d-flex gap-3">
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
                        <Icon icon="document-group" addon="edit" /> Customize imported collections
                    </Button>
                </div>
                {!isImportAll && (
                    <div className="mt-4">
                        <div className="mb-2">
                            {/* typing filters the list below AND offers an "Add ..." create option */}
                            <SelectCreatable<CollectionOption>
                                placeholder="Type a collection name from the imported file to add it"
                                options={[]}
                                inputValue={collectionFilter}
                                onInputChange={(value, meta) => {
                                    if (meta.action === "input-change") {
                                        setCollectionFilter(value);
                                    }
                                }}
                                onChange={onCollectionPicked}
                                isValidNewOption={isNewCollection}
                                noOptionsMessage={() => "Type a collection name to add it"}
                                formatCreateLabel={(input) => `Add "${input.trim()}"`}
                                isClearedAfterSelect
                                maxMenuHeight={300}
                            />
                        </div>
                        <div className="import-collections-list">
                            <Table className="mb-0">
                                <thead>
                                    <tr>
                                        <th>Collection name</th>
                                        <th className="text-end">
                                            <Button
                                                variant="link"
                                                size="sm"
                                                className="p-0"
                                                disabled={filteredCollections.length === 0}
                                                onClick={() =>
                                                    setValue(
                                                        "collections.includedCollections",
                                                        areAllFilteredCollectionsSelected
                                                            ? includedCollections.filter(
                                                                  (name) => !filteredCollections.includes(name)
                                                              )
                                                            : [
                                                                  ...includedCollections,
                                                                  ...filteredCollections.filter(
                                                                      (name) => !includedCollections.includes(name)
                                                                  ),
                                                              ],
                                                        { shouldDirty: true }
                                                    )
                                                }
                                            >
                                                {areAllFilteredCollectionsSelected ? "Deselect all" : "Select all"}
                                            </Button>
                                        </th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {filteredCollections.length === 0 && (
                                        <tr>
                                            <td colSpan={2} className="text-muted">
                                                No collections added. Type a collection name from the imported file
                                                above and pick &quot;Add&quot;.
                                            </td>
                                        </tr>
                                    )}
                                    {filteredCollections.map((name) => (
                                        <tr key={name}>
                                            <td colSpan={2}>
                                                <div className="d-flex align-items-center justify-content-between">
                                                    <Form.Check
                                                        type="switch"
                                                        label={name}
                                                        checked={includedCollections.includes(name)}
                                                        onChange={(e) => toggleCollection(name, e.target.checked)}
                                                    />
                                                    <Button
                                                        variant="link"
                                                        size="sm"
                                                        className="p-0 text-danger"
                                                        title="Remove collection"
                                                        onClick={() => removeCollection(name)}
                                                    >
                                                        <Icon icon="trash" margin="m-0" />
                                                    </Button>
                                                </div>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </Table>
                        </div>
                    </div>
                )}
            </Card>

            <div className="d-flex justify-content-between align-items-center mb-2">
                <div id="documents-and-extensions" className="small-label">
                    Select documents and extensions
                </div>
                <Button variant="link" size="sm" onClick={() => setAllDocuments(!areAllDocumentsSelected)}>
                    {areAllDocumentsSelected ? "Deselect all" : "Select all"}
                </Button>
            </div>
            <Card className="p-4">
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
                    <PopoverWithHoverWrapper message="Importing artificial documents might cause import error of Map-Reduce indexes with OutputReduceToCollection.">
                        <span onClick={(e) => e.preventDefault()}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </span>
                    </PopoverWithHoverWrapper>
                </FormSwitch>
                <div
                    className="d-flex align-items-center gap-2"
                    title={
                        isArchivedRestricted
                            ? getDocumentToggleRestrictionTooltip("isIncludeArchivedDocuments")
                            : undefined
                    }
                >
                    <div className={isArchivedRestricted ? "item-disabled" : undefined}>
                        <FormSwitch
                            control={control}
                            name="documents.isIncludeArchivedDocuments"
                            {...(isArchivedRestricted && { disabled: true })}
                        >
                            Include Archived Documents
                        </FormSwitch>
                    </div>
                    {isArchivedRestricted && (
                        <LicenseRestrictedBadge
                            licenseRequired={getDocumentToggleLicenseRequired("isIncludeArchivedDocuments")}
                        />
                    )}
                </div>
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
            </Card>
        </ImportSection>
    );
}

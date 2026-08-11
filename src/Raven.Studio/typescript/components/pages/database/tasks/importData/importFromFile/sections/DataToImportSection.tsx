import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import { documentToggleKeys, ImportFromFileFormData } from "../importFromFileValidation";
import { getItemsToWarnAbout } from "../importFromFileUtils";
import { useImportRestrictions } from "../useImportRestrictions";
import RestrictedSwitch from "./RestrictedSwitch";
import CollectionsToImportPicker from "./CollectionsToImportPicker";
import Card from "react-bootstrap/Card";
import classNames from "classnames";

export default function DataToImportSection() {
    const { control, setValue } = useFormContext<ImportFromFileFormData>();
    const documents = useWatch({ control, name: "documents" });
    const { documentToggles: documentToggleRestrictions } = useImportRestrictions();

    const isImportAll = useWatch({ control, name: "collections.isImportAllCollections" });

    const itemsToWarnAbout = getItemsToWarnAbout({ documents });

    const forceDocumentsOn = (value: boolean) => {
        if (value) {
            setValue("documents.isIncludeDocuments", true, { shouldDirty: true });
        }
    };

    const selectableDocumentToggleNames = documentToggleKeys.filter((name) => !documentToggleRestrictions[name]);

    const areAllDocumentsSelected = selectableDocumentToggleNames.every((name) => documents[name]);

    const setAllDocuments = (value: boolean) => {
        selectableDocumentToggleNames.forEach((name) => setValue(`documents.${name}`, value, { shouldDirty: true }));
    };

    return (
        <ImportSection id="data-to-import" title="Data to import">
            <div id="collections-to-import" className="small-label mb-2">
                Choose collections to import
            </div>
            <Card className="mb-4 p-4">
                <div className="d-flex gap-2">
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: isImportAll })}
                        onClick={() => setValue("collections.isImportAllCollections", true, { shouldDirty: true })}
                    >
                        <Icon icon="documents" margin="m-0" />
                        Import all collections
                    </button>
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: !isImportAll })}
                        onClick={() => setValue("collections.isImportAllCollections", false, { shouldDirty: true })}
                    >
                        <Icon icon="document-group" addon="edit" margin="m-0" />
                        Customize imported collections
                    </button>
                </div>
                {!isImportAll && <CollectionsToImportPicker />}
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
                <FormSwitch control={control} name="documents.isIncludeDocuments" className="pb-1">
                    Include Documents
                </FormSwitch>
                <div className="ms-4 d-flex flex-column gap-1">
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
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeArtificialDocuments">
                    Include Artificial Documents{" "}
                    <PopoverWithHoverWrapper message="Importing artificial documents might cause import error of Map-Reduce indexes with OutputReduceToCollection.">
                        <span onClick={(e) => e.preventDefault()}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </span>
                    </PopoverWithHoverWrapper>
                </FormSwitch>
                <hr className="my-1" />
                <RestrictedSwitch
                    control={control}
                    name="documents.isIncludeArchivedDocuments"
                    restriction={documentToggleRestrictions.isIncludeArchivedDocuments}
                >
                    Include Archived Documents
                </RestrictedSwitch>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeExpiredDocuments">
                    Include Expired Documents
                </FormSwitch>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeConflicts">
                    Include Conflicts
                </FormSwitch>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeCompareExchange">
                    Include Compare Exchange
                </FormSwitch>
                <hr className="my-1" />
                <FormSwitch control={control} name="documents.isIncludeCompareExchangeTombstones">
                    Include Compare Exchange Tombstones
                </FormSwitch>
                <hr className="my-1" />
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

import "./ImportDatabaseFromFile.scss";
import React, { useEffect, useRef, useState } from "react";
import { FieldErrors, FormProvider, useWatch } from "react-hook-form";
import { useAsync } from "react-async-hook";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import ProgressBar from "react-bootstrap/ProgressBar";
import classNames from "classnames";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useScrollSpy } from "components/hooks/useScrollSpy";
import viewHelpers = require("common/helpers/view/viewHelpers");
import { useImportFromFileForm } from "./useImportFromFileForm";
import { useImportOperation } from "./useImportOperation";
import { ImportRestrictionsProvider, useImportRestrictions } from "./useImportRestrictions";
import { hasAnyInclude } from "./importFromFileUtils";
import { ImportFromFileFormData } from "./importFromFileValidation";
import { getFirstErrorPath, getSectionIdForErrorPath, sectionIds } from "./importFromFileNav";
import ImportSideNav from "./ImportSideNav";
import SelectFileSection from "./sections/SelectFileSection";
import DataToImportSection from "./sections/DataToImportSection";
import ConfigurationToImportSection from "./sections/ConfigurationToImportSection";
import ImportProcessingSection from "./sections/ImportProcessingSection";
import ImportResultModal from "./ImportResultModal";
import ImportCommandModal from "./ImportCommandModal";

export default function ImportDatabaseFromFile() {
    return (
        <ImportRestrictionsProvider>
            <ImportDatabaseFromFileContent />
        </ImportRestrictionsProvider>
    );
}

function ImportDatabaseFromFileContent() {
    const { forCurrentDatabase } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const { restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys } =
        useImportRestrictions();

    const form = useImportFromFileForm();
    const { control, handleSubmit, formState } = form;
    const file = useWatch({ control, name: "file" });

    const { startImport, uploadPercent, isUploading, operationState, isResultModalOpen, closeResultModal } =
        useImportOperation();

    const [isCommandModalOpen, setIsCommandModalOpen] = useState(false);

    useDirtyFlag(isUploading, uploadInProgressDialog);

    const contentRef = useRef<HTMLDivElement>(null);
    const [scrollRoot, setScrollRoot] = useState<Element | null>(null);
    useEffect(() => {
        setScrollRoot(contentRef.current);
    }, []);

    const { activeId: activeSectionId, selectSection } = useScrollSpy(sectionIds, { root: scrollRoot });

    const importOptionsUrl = forCurrentDatabase.importDataOptionsUrl();

    const asyncEssentialStats = useAsync(
        async () => (databaseName ? databasesService.getEssentialStats(databaseName) : null),
        [databaseName]
    );

    const hasExistingData =
        !asyncEssentialStats.result ||
        asyncEssentialStats.result.CountOfDocuments > 0 ||
        asyncEssentialStats.result.CountOfIndexes > 0;

    const onInvalidSubmit = (errors: FieldErrors<ImportFromFileFormData>) => {
        const firstErrorPath = getFirstErrorPath(errors);
        if (!firstErrorPath) {
            return;
        }

        const sectionId = getSectionIdForErrorPath(firstErrorPath);
        if (!sectionId) {
            return;
        }

        selectSection(sectionId);
    };

    const watchedDocuments = useWatch({ control, name: "documents" });
    const watchedConfiguration = useWatch({ control, name: "configuration" });

    const hasInclude = hasAnyInclude(
        { documents: watchedDocuments, configuration: watchedConfiguration } as ImportFromFileFormData,
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys
    );

    // The button stays enabled while the form is invalid so the click can point the user at the field
    const canImport = !!file && hasInclude && !isUploading && !formState.isSubmitting;

    return (
        <FormProvider {...form}>
            <div className="import-page">
                <AboutViewHeading
                    title="Import data from a .ravendbdump file into the current database"
                    icon="import-database"
                    backUrl={importOptionsUrl}
                    marginBottom={2}
                />
                {hasExistingData && (
                    <Alert variant="warning" className="w-50">
                        <Icon icon="warning" /> Note: Importing will overwrite any existing documents and indexes.
                    </Alert>
                )}
                <div className="my-4 d-flex align-items-center gap-3">
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        disabled={!canImport}
                        onClick={handleSubmit(startImport, onInvalidSubmit)}
                    >
                        <Icon icon="import-database" /> Import database
                    </Button>
                    <Button
                        variant="secondary"
                        className="rounded-pill"
                        disabled={!file}
                        onClick={() => setIsCommandModalOpen(true)}
                    >
                        <Icon icon="code" /> Use import command
                    </Button>
                </div>
                {isUploading && (
                    <ProgressBar
                        now={uploadPercent}
                        striped
                        animated
                        label={`${uploadPercent}%`}
                        visuallyHidden
                        className="mb-3"
                    />
                )}
                {!hasInclude && (
                    <Alert variant="warning" className="mb-2">
                        Note: At least one &apos;include&apos; option must be checked.
                    </Alert>
                )}
                <div className="d-flex gap-4 import-page-body">
                    <ImportSideNav activeSectionId={activeSectionId} onSelectSection={selectSection} />
                    <div className="flex-grow-1 import-page-content" ref={contentRef}>
                        <SelectFileSection />
                        <fieldset disabled={!file} className={classNames({ "item-disabled": !file })}>
                            <DataToImportSection />
                            <ConfigurationToImportSection />
                            <ImportProcessingSection />
                        </fieldset>
                    </div>
                </div>
                {isCommandModalOpen && <ImportCommandModal onClose={() => setIsCommandModalOpen(false)} />}
                {isResultModalOpen && operationState && (
                    <ImportResultModal
                        progress={operationState.progress}
                        status={operationState.status}
                        startTime={operationState.startTime}
                        endTime={operationState.endTime}
                        onClose={closeResultModal}
                    />
                )}
            </div>
        </FormProvider>
    );
}

function uploadInProgressDialog(): JQueryDeferred<confirmDialogResult> {
    const result = $.Deferred<confirmDialogResult>();

    viewHelpers
        .confirmationMessage("Upload is in progress", "Please wait until uploading is complete.", {
            buttons: ["OK"],
        })
        .always(() => result.resolve({ can: false }));

    return result;
}

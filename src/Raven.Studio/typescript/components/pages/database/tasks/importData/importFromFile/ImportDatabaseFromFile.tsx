import React, { useState } from "react";
import { FieldErrors, FormProvider, useWatch } from "react-hook-form";
import { useAsync } from "react-async-hook";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import ProgressBar from "react-bootstrap/ProgressBar";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import viewHelpers = require("common/helpers/view/viewHelpers");
import { useImportFromFileForm } from "./useImportFromFileForm";
import { useImportOperation } from "./useImportOperation";
import {
    ImportRestrictionsProvider,
    useImportRestrictions,
} from "components/pages/database/tasks/importData/shared/useImportRestrictions";
import { hasAnyInclude } from "./importFromFileUtils";
import { ImportFromFileFormData } from "./importFromFileValidation";
import { errorFieldTargets, sectionNav } from "./importFromFileNav";
import {
    getFirstErrorPath,
    getSectionIdForErrorPath,
} from "components/pages/database/tasks/importData/shared/importNav";
import ImportPageLayout from "components/pages/database/tasks/importData/shared/ImportPageLayout";
import { useImportPageScroll } from "components/pages/database/tasks/importData/shared/useImportPageScroll";
import SelectFileSection from "./sections/SelectFileSection";
import DataToImportSection from "./sections/DataToImportSection";
import ConfigurationToImportSection from "components/pages/database/tasks/importData/shared/ConfigurationToImportSection";
import ImportProcessingSection from "./sections/ImportProcessingSection";
import ImportResultModal from "components/pages/database/tasks/importData/shared/ImportResultModal";
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

    const scroll = useImportPageScroll(sectionNav);

    const importOptionsUrl = forCurrentDatabase.importDataOptionsUrl();

    const asyncEssentialStats = useAsync(
        async () => (databaseName ? databasesService.getEssentialStats(databaseName) : null),
        [databaseName]
    );

    const hasExistingData =
        asyncEssentialStats.result?.CountOfDocuments > 0 || asyncEssentialStats.result?.CountOfIndexes > 0;

    const onInvalidSubmit = (errors: FieldErrors<ImportFromFileFormData>) => {
        const firstErrorPath = getFirstErrorPath(errors);
        if (!firstErrorPath) {
            return;
        }

        const sectionId = getSectionIdForErrorPath(firstErrorPath, errorFieldTargets);
        if (!sectionId) {
            return;
        }

        scroll.selectSection(sectionId);
    };

    const watchedDocuments = useWatch({ control, name: "documents" });
    const watchedConfiguration = useWatch({ control, name: "configuration" });

    const hasInclude = hasAnyInclude(
        { documents: watchedDocuments, configuration: watchedConfiguration },
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys
    );

    const canImport = !!file && hasInclude && !isUploading && !formState.isSubmitting;

    return (
        <FormProvider {...form}>
            <ImportPageLayout
                title="Import data from a .ravendbdump file"
                icon="import-database"
                backUrl={importOptionsUrl}
                navItems={sectionNav}
                scroll={scroll}
                alert={
                    hasExistingData && (
                        <Alert variant="warning" className="w-50">
                            <Icon icon="warning" color="warning" /> Note: Importing will overwrite any existing
                            documents and indexes.
                        </Alert>
                    )
                }
                actions={
                    <>
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
                    </>
                }
                banner={
                    <>
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
                    </>
                }
            >
                <SelectFileSection />
                <fieldset disabled={!file} className={classNames({ "item-disabled": !file })}>
                    <DataToImportSection />
                    <ConfigurationToImportSection />
                    <ImportProcessingSection />
                </fieldset>
            </ImportPageLayout>
            {isCommandModalOpen && <ImportCommandModal onClose={() => setIsCommandModalOpen(false)} />}
            {isResultModalOpen && operationState && (
                <ImportResultModal
                    progress={operationState.progress}
                    error={operationState.error}
                    status={operationState.status}
                    startTime={operationState.startTime}
                    endTime={operationState.endTime}
                    onClose={closeResultModal}
                />
            )}
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

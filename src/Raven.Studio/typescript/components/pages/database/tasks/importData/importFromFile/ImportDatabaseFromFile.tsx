import "./ImportDatabaseFromFile.scss";
import React, { useEffect, useRef, useState } from "react";
import { FieldErrors, FieldPath, FormProvider, useWatch } from "react-hook-form";
import { useAsync } from "react-async-hook";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import classNames from "classnames";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useScrollSpy } from "components/hooks/useScrollSpy";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import notificationCenter from "common/notifications/notificationCenter";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import messagePublisher from "common/messagePublisher";
import viewHelpers = require("common/helpers/view/viewHelpers");
import { useImportFromFileForm } from "./useImportFromFileForm";
import { useImportRestrictions } from "./useImportRestrictions";
import { hasAnyInclude, toImportDto } from "./importFromFileUtils";
import { ImportFromFileFormData } from "./importFromFileValidation";
import SelectFileSection from "./sections/SelectFileSection";
import DataToImportSection from "./sections/DataToImportSection";
import ConfigurationToImportSection from "./sections/ConfigurationToImportSection";
import ImportProcessingSection from "./sections/ImportProcessingSection";
import ImportResultModal from "./ImportResultModal";
import ImportCommandModal from "./ImportCommandModal";
import IconName from "typings/server/icons";

type SmugglerProgress = Raven.Client.Documents.Smuggler.SmugglerProgressBase;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

interface OperationState {
    progress: SmugglerProgress | null;
    status: OperationStatus;
    startTime: Date;
    endTime: Date | null;
}

export default function ImportDatabaseFromFile() {
    const { forCurrentDatabase } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService, databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys } =
        useImportRestrictions();

    const form = useImportFromFileForm();
    const { control, handleSubmit, formState } = form;
    const file = useWatch({ control, name: "file" });

    const [uploadPercent, setUploadPercent] = useState<number | null>(null);
    const [operationState, setOperationState] = useState<OperationState | null>(null);
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [isCommandModalOpen, setIsCommandModalOpen] = useState(false);

    const isUploading = uploadPercent != null;
    useDirtyFlag(isUploading, uploadInProgressDialog);

    // monitorOperation gives no way to detach its callbacks, so guard them manually - otherwise
    // navigating away mid-import keeps calling setState on the unmounted view
    const isUnmountedRef = useRef(false);
    useEffect(() => {
        isUnmountedRef.current = false;
        return () => {
            isUnmountedRef.current = true;
        };
    }, []);

    const contentRef = useRef<HTMLDivElement>(null);
    const [scrollRoot, setScrollRoot] = useState<Element | null>(null);
    useEffect(() => {
        setScrollRoot(contentRef.current);
    }, []);

    const activeSectionId = useScrollSpy(sectionIds, { root: scrollRoot });

    const importOptionsUrl = forCurrentDatabase.importDataOptionsUrl();
    const importDocsLink = useRavenLink({ hash: "YD9M1R" });

    const asyncEssentialStats = useAsync(async () => databasesService.getEssentialStats(databaseName), [databaseName]);
    const essentialStats = asyncEssentialStats.result;
    // Only a successful response with actual counts proves the database is empty; anything else
    // (still loading, failed, or a response we cannot read) keeps the warning visible.
    const hasExistingData = essentialStats?.CountOfDocuments > 0 || essentialStats?.CountOfIndexes > 0;

    const onSubmit = async (formData: ImportFromFileFormData) => {
        if (
            !hasAnyInclude(formData, restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys)
        ) {
            return;
        }

        reportEvent("database", "import");

        const dto = toImportDto(
            formData,
            restrictedSettingKeys,
            restrictedOngoingTaskKeys,
            restrictedConnectionStringKeys
        );

        try {
            await tasksService.validateSmugglerOptions(
                {
                    TransformScript: dto.TransformScript,
                } as Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide,
                databaseName
            );
        } catch (error) {
            messagePublisher.reportError(
                "Invalid import options",
                error?.responseText ?? String(error),
                error?.statusText
            );
            return;
        }

        let operationId: number;
        try {
            operationId = await tasksService.getNextOperationId(databaseName);
        } catch (error) {
            messagePublisher.reportError(
                "Could not get next task id.",
                error?.responseText ?? String(error),
                error?.statusText
            );
            return;
        }

        const startTime = new Date();

        setOperationState({
            progress: null,
            status: "InProgress",
            startTime,
            endTime: null,
        });
        setIsModalOpen(true);
        setUploadPercent(0);

        const monitor = notificationCenter.instance.monitorOperation<SmugglerProgress>(
            databaseName,
            operationId,
            (progress) => {
                if (!isUnmountedRef.current) {
                    setOperationState((prev) => (prev ? { ...prev, progress } : prev));
                }
            }
        );

        monitor
            .done((result: SmugglerProgress) => {
                if (!isUnmountedRef.current) {
                    setOperationState((prev) =>
                        prev ? { ...prev, progress: result, status: "Completed", endTime: new Date() } : prev
                    );
                }
            })
            .fail(() => {
                if (!isUnmountedRef.current) {
                    setOperationState((prev) => (prev ? { ...prev, status: "Faulted", endTime: new Date() } : prev));
                }
            });

        // Knockout parity: refresh revisions config when import enabled it
        const db = activeDatabaseTracker.default.database();
        if (db && !db.hasRevisionsConfiguration()) {
            monitor.done(async () => {
                // a rejection here would surface as an unhandled promise rejection inside a
                // jQuery done callback - the refresh is best-effort, so swallow the failure
                try {
                    const dbInfo = await tasksService.getDatabaseForStudio(databaseName);
                    if (dbInfo.HasRevisionsConfiguration) {
                        db.hasRevisionsConfiguration(true);
                        collectionsTracker.default.configureRevisions(db);
                    }
                } catch {
                    // ignore - the revisions config will refresh on the next full load
                }
            });
        }

        try {
            await tasksService.importDatabaseFromFile(databaseName, operationId, formData.file, dto, (percent) =>
                setUploadPercent(Math.round(percent))
            );
        } catch {
            // the command reports the upload error itself; if the upload died before the server
            // registered any progress, monitorOperation will never settle - mark Faulted ourselves
            if (!isUnmountedRef.current) {
                setOperationState((prev) =>
                    prev && prev.status === "InProgress" && !prev.progress
                        ? { ...prev, status: "Faulted", endTime: new Date() }
                        : prev
                );
            }
        } finally {
            if (!isUnmountedRef.current) {
                setUploadPercent(null);
            }
        }
    };

    const onInvalidSubmit = (errors: FieldErrors<ImportFromFileFormData>) => {
        const firstErrorPath = getFirstErrorPath(errors);
        if (!firstErrorPath) {
            return;
        }

        const target = errorFieldTargets.find((x) => firstErrorPath.startsWith(x.path));
        if (!target) {
            return;
        }

        target.revealToggles?.forEach((toggle) => form.setValue(toggle, true));
        // let the Collapse mount its content before scrolling to it
        requestAnimationFrame(() => scrollToSection(target.sectionId));
    };

    // only these two groups feed hasAnyInclude - watching the whole form here would re-render every
    // section on each keystroke in the transform-script editor
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
                        onClick={handleSubmit(onSubmit, onInvalidSubmit)}
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
                {!hasInclude && (
                    <Alert variant="warning">Note: At least one &apos;include&apos; option must be checked.</Alert>
                )}
                <div className="d-flex gap-4 import-page-body">
                    <nav className="import-side-nav align-self-start">
                        {sectionNav.map((item, index) => (
                            <React.Fragment key={item.id}>
                                {index > 0 && <hr />}
                                <ImportSideNavItem item={item} activeSectionId={activeSectionId} />
                            </React.Fragment>
                        ))}
                    </nav>
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
                {isModalOpen && operationState && (
                    <ImportResultModal
                        progress={operationState.progress}
                        status={operationState.status}
                        startTime={operationState.startTime}
                        endTime={operationState.endTime}
                        onClose={() => setIsModalOpen(false)}
                    />
                )}
            </div>
        </FormProvider>
    );
}

interface SectionNavItem {
    id: string;
    label: string;
    icon: IconName;
    children?: { id: string; label: string }[];
}

const sectionNav: SectionNavItem[] = [
    { id: "select-file", label: "Select file to import", icon: "folder" },
    {
        id: "data-to-import",
        label: "Data to import",
        icon: "document",
        children: [
            { id: "collections-to-import", label: "Collections to import" },
            { id: "documents-and-extensions", label: "Documents and extensions" },
        ],
    },
    {
        id: "configuration-to-import",
        label: "Configuration to import",
        icon: "database",
        children: [
            { id: "database-entities", label: "Database entities" },
            { id: "database-settings", label: "Database settings" },
        ],
    },
    { id: "import-processing", label: "Import processing & security", icon: "settings" },
];

const sectionIds = sectionNav.flatMap((item) => [item.id, ...(item.children?.map((child) => child.id) ?? [])]);

function scrollToSection(id: string) {
    document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
}

interface ImportSideNavItemProps {
    item: SectionNavItem;
    activeSectionId: string | null;
}

function ImportSideNavItem({ item, activeSectionId }: ImportSideNavItemProps) {
    return (
        <>
            <button
                type="button"
                className={classNames("import-side-nav-item", {
                    active: activeSectionId === item.id,
                })}
                onClick={() => scrollToSection(item.id)}
            >
                <Icon icon={item.icon} margin="m-0" /> {item.label}
            </button>
            {item.children?.map((child) => (
                <button
                    key={child.id}
                    type="button"
                    className={classNames("import-side-nav-item import-side-nav-subitem", {
                        active: activeSectionId === child.id,
                    })}
                    onClick={() => scrollToSection(child.id)}
                >
                    {child.label}
                </button>
            ))}
        </>
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

function getFirstErrorPath(errors: unknown, prefix = ""): string | null {
    if (!errors || typeof errors !== "object") {
        return null;
    }
    if ("message" in errors && typeof (errors as { message?: unknown }).message === "string") {
        return prefix;
    }
    for (const [key, value] of Object.entries(errors)) {
        const path = prefix ? `${prefix}.${key}` : key;
        const found = getFirstErrorPath(value, path);
        if (found) {
            return found;
        }
    }
    return null;
}

const errorFieldTargets: {
    path: string;
    sectionId: string;
    revealToggles?: FieldPath<ImportFromFileFormData>[];
}[] = [
    {
        path: "processing.transformScript",
        sectionId: "import-processing",
        revealToggles: ["processing.isUseTransformScript"],
    },
    {
        path: "processing.maxReadOpsPerSecond",
        sectionId: "import-processing",
        revealToggles: ["processing.isSetMaxReadOpsPerSecond"],
    },
    {
        path: "processing.encryptionKey",
        sectionId: "import-processing",
        revealToggles: ["processing.isEncrypted"],
    },
    { path: "processing", sectionId: "import-processing" },
    { path: "file", sectionId: "select-file" },
    { path: "documents", sectionId: "data-to-import" },
    { path: "collections", sectionId: "data-to-import" },
    { path: "configuration", sectionId: "configuration-to-import" },
];

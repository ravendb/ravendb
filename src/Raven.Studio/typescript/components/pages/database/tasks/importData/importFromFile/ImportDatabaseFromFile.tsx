import "./ImportDatabaseFromFile.scss";
import React, { useEffect, useRef, useState } from "react";
import { FormProvider } from "react-hook-form";
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
import { useEventsCollector } from "components/hooks/useEventsCollector";
import notificationCenter from "common/notifications/notificationCenter";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import messagePublisher from "common/messagePublisher";
import { useImportFromFileForm } from "./useImportFromFileForm";
import { useImportLicenseRestrictions } from "./useImportLicenseRestrictions";
import { toImportDto, hasAnyInclude } from "./importFromFileUtils";
import { ImportFromFileFormData } from "./importFromFileValidation";
import SelectFileSection from "./sections/SelectFileSection";
import DataToImportSection from "./sections/DataToImportSection";
import ConfigurationToImportSection from "./sections/ConfigurationToImportSection";
import ImportProcessingSection from "./sections/ImportProcessingSection";
import ImportResultModal from "./ImportResultModal";
import IconName from "typings/server/icons";

type SmugglerProgress = Raven.Client.Documents.Smuggler.SmugglerProgressBase;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

const sectionIds = ["select-file", "data-to-import", "configuration-to-import", "import-processing"];

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

function scrollToSection(id: string) {
    document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
}

function findScrollParent(element: HTMLElement | null): Element | null {
    let current = element?.parentElement;
    while (current && current !== document.body) {
        const { overflowY } = getComputedStyle(current);
        if ((overflowY === "auto" || overflowY === "scroll") && current.scrollHeight > current.clientHeight) {
            return current;
        }
        current = current.parentElement;
    }
    return null;
}

interface OperationState {
    operationId: number;
    databaseName: string;
    progress: SmugglerProgress | null;
    status: OperationStatus;
    startTime: Date;
    endTime: Date | null;
}

export default function ImportDatabaseFromFile() {
    const { forCurrentDatabase } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { restrictedFeatures } = useImportLicenseRestrictions();
    const restrictedKeys = restrictedFeatures.map((x) => x.settingKey);

    const form = useImportFromFileForm();
    const { handleSubmit, watch, formState } = form;
    const file = watch("file");

    const [uploadPercent, setUploadPercent] = useState<number | null>(null);
    const [operationState, setOperationState] = useState<OperationState | null>(null);
    const [isModalOpen, setIsModalOpen] = useState(false);

    const isUploading = uploadPercent != null;
    useDirtyFlag(isUploading);

    // The Studio scrolls the view inside a container (not the document), so the scroll-spy needs
    // that container as its root - otherwise its bottom-of-scroll fallback always fires.
    const pageRef = useRef<HTMLDivElement>(null);
    const [scrollRoot, setScrollRoot] = useState<Element | null>(null);
    useEffect(() => {
        setScrollRoot(findScrollParent(pageRef.current));
    }, []);

    const activeSectionId = useScrollSpy(sectionIds, { root: scrollRoot });

    const importOptionsUrl = forCurrentDatabase.importDataOptionsUrl();

    const onSubmit = async (formData: ImportFromFileFormData) => {
        if (!hasAnyInclude(formData, restrictedKeys)) {
            return; // guarded by disabled submit; double check
        }

        reportEvent("database", "import");

        const dto = toImportDto(formData, restrictedKeys);

        try {
            await tasksService.validateSmugglerOptions(
                { TransformScript: dto.TransformScript } as Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide,
                databaseName
            );
        } catch (error: any) {
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
        } catch (error: any) {
            messagePublisher.reportError(
                "Could not get next task id.",
                error?.responseText ?? String(error),
                error?.statusText
            );
            return;
        }

        const startTime = new Date();

        setOperationState({
            operationId,
            databaseName,
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
            (progress) => setOperationState((prev) => (prev ? { ...prev, progress } : prev))
        );

        monitor
            .done((result: SmugglerProgress) =>
                setOperationState((prev) =>
                    prev ? { ...prev, progress: result, status: "Completed", endTime: new Date() } : prev
                )
            )
            .fail(() =>
                setOperationState((prev) => (prev ? { ...prev, status: "Faulted", endTime: new Date() } : prev))
            );

        // Knockout parity: refresh revisions config when import enabled it
        const db = activeDatabaseTracker.default.database();
        if (db && !db.hasRevisionsConfiguration()) {
            monitor.done(async () => {
                const dbInfo = await tasksService.getDatabaseForStudio(databaseName);
                if (dbInfo.HasRevisionsConfiguration) {
                    db.hasRevisionsConfiguration(true);
                    collectionsTracker.default.configureRevisions(db);
                }
            });
        }

        try {
            await tasksService.importDatabaseFromFile(databaseName, operationId, formData.file, dto, (percent) =>
                setUploadPercent(percent)
            );
        } catch {
            // the command reports the upload error itself; if the upload died before the server
            // registered any progress, monitorOperation will never settle - mark Faulted ourselves
            setOperationState((prev) =>
                prev && prev.status === "InProgress" && !prev.progress
                    ? { ...prev, status: "Faulted", endTime: new Date() }
                    : prev
            );
        } finally {
            setUploadPercent(null);
        }
    };

    const watchedFormData = watch();
    const canImport = !!file && hasAnyInclude(watchedFormData, restrictedKeys) && !isUploading && formState.isValid;

    return (
        <FormProvider {...form}>
            <div className="content-margin" ref={pageRef}>
                <AboutViewHeading
                    title="Import data from a .ravendbdump file into the current database"
                    icon="import-database"
                    // marginBottom={5}
                    backUrl={importOptionsUrl}
                />
                <Alert variant="info" className="w-50">
                    <Icon icon="info" /> Note: Importing will overwrite any existing documents and indexes.
                </Alert>
                <div className="my-4 d-flex align-items-center gap-3">
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        disabled={!canImport}
                        onClick={handleSubmit(onSubmit)}
                    >
                        <Icon icon="import-database" /> Import database
                    </Button>
                    {uploadPercent != null && (
                        <div style={{ minWidth: 220 }}>
                            <ProgressBar animated now={uploadPercent} label={`${uploadPercent}%`} />
                        </div>
                    )}
                </div>
                {!hasAnyInclude(watchedFormData, restrictedKeys) && (
                    <Alert variant="warning">Note: At least one &apos;include&apos; option must be checked.</Alert>
                )}
                <div className="d-flex gap-4">
                    <nav className="import-side-nav position-sticky align-self-start" style={{ top: 20 }}>
                        {sectionNav.map((item, index) => (
                            <React.Fragment key={item.id}>
                                {index > 0 && <hr />}
                                {/* plain buttons on purpose: anchor hrefs would change the location
                                    hash, which the Studio router interprets as a route change */}
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
                                        className="import-side-nav-item import-side-nav-subitem"
                                        onClick={() => scrollToSection(child.id)}
                                    >
                                        {child.label}
                                    </button>
                                ))}
                            </React.Fragment>
                        ))}
                    </nav>
                    <div className="flex-grow-1">
                        <SelectFileSection restrictedFeatures={restrictedFeatures} />
                        <fieldset disabled={!file} className={classNames({ "item-disabled": !file })}>
                            <DataToImportSection />
                            <ConfigurationToImportSection />
                            <ImportProcessingSection />
                        </fieldset>
                    </div>
                </div>
                {isModalOpen && operationState && (
                    <ImportResultModal
                        progress={operationState.progress}
                        status={operationState.status}
                        startTime={operationState.startTime}
                        endTime={operationState.endTime}
                        onClose={() => setIsModalOpen(false)}
                        onShowDetails={() =>
                            notificationCenter.instance.openDetailsForOperationById(
                                operationState.databaseName,
                                operationState.operationId
                            )
                        }
                    />
                )}
            </div>
        </FormProvider>
    );
}

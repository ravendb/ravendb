import React, { ReactNode } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import RestrictedSwitch from "components/pages/database/tasks/importData/shared/RestrictedSwitch";
import { getLicenseLimitWarning } from "components/pages/database/tasks/importData/shared/importLicenseLimits";
import {
    DocumentToggleKey,
    documentToggleKeys,
    ImportOptionsFormData,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import { useImportRestrictions } from "components/pages/database/tasks/importData/shared/useImportRestrictions";

interface DocumentsToImportCardProps {
    visibleToggles?: readonly DocumentToggleKey[];
}

interface ToggleExtras {
    label?: ReactNode;
    afterChange?: (value: boolean) => void;
    warning?: string;
    className?: string;
}

export default function DocumentsToImportCard({ visibleToggles = documentToggleKeys }: DocumentsToImportCardProps) {
    const t = useStudioTranslation("importData");
    const { control, setValue } = useFormContext<ImportOptionsFormData>();
    const documents = useWatch({ control, name: "documents" });
    const { documentToggles: restrictions } = useImportRestrictions();
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const subscriptionsLimitWarning = getLicenseLimitWarning(licenseStatus, "subscriptions");

    const forceDocumentsOn = (value: boolean) => {
        if (value) {
            setValue("documents.isIncludeDocuments", true, { shouldDirty: true });
        }
    };

    const selectableToggleKeys = visibleToggles.filter((key) => !restrictions[key]);
    const areAllSelected = selectableToggleKeys.every((key) => documents[key]);

    const setAll = (value: boolean) => {
        selectableToggleKeys.forEach((key) => setValue(`documents.${key}`, value, { shouldDirty: true }));
    };

    const renderToggle = (key: DocumentToggleKey, extras?: ToggleExtras) =>
        visibleToggles.includes(key) && (
            <RestrictedSwitch
                key={key}
                control={control}
                name={`documents.${key}`}
                restriction={restrictions[key]}
                afterChange={extras?.afterChange}
                warning={extras?.warning}
                className={extras?.className}
            >
                {extras?.label ?? t(`documents.toggles.${key}`)}
            </RestrictedSwitch>
        );

    const documentsToggle = renderToggle("isIncludeDocuments", { className: "pb-1" });
    const extensionToggles = [
        renderToggle("isIncludeAttachments", { afterChange: forceDocumentsOn }),
        renderToggle("isIncludeCounters", { afterChange: forceDocumentsOn }),
        renderToggle("isIncludeRevisions", { afterChange: forceDocumentsOn }),
        renderToggle("isIncludeTimeSeries", { afterChange: forceDocumentsOn }),
        renderToggle("isIncludeTimeSeriesDeletedRanges"),
        renderToggle("isIncludeLegacyAttachments"),
        renderToggle("isIncludeDocumentsTombstones"),
    ].filter(Boolean);

    const groups = [
        (documentsToggle || extensionToggles.length > 0) && (
            <>
                {documentsToggle}
                {extensionToggles.length > 0 && <div className="ms-4 d-flex flex-column gap-1">{extensionToggles}</div>}
            </>
        ),
        renderToggle("isIncludeArtificialDocuments", {
            label: (
                <>
                    {t("documents.toggles.isIncludeArtificialDocuments")}{" "}
                    <PopoverWithHoverWrapper message={t("documents.artificialDocumentsInfo")}>
                        <span onClick={(e) => e.preventDefault()}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </span>
                    </PopoverWithHoverWrapper>
                </>
            ),
        }),
        renderToggle("isIncludeArchivedDocuments"),
        renderToggle("isIncludeExpiredDocuments"),
        renderToggle("isIncludeConflicts"),
        renderToggle("isIncludeCompareExchange"),
        renderToggle("isIncludeCompareExchangeTombstones"),
        renderToggle("isIncludeSubscriptions", { warning: subscriptionsLimitWarning }),
    ].filter(Boolean);

    return (
        <>
            <div className="d-flex justify-content-between align-items-center mb-2">
                <h4 id="documents-and-extensions" className="m-0">
                    {t("documents.title")}
                </h4>
                <Button variant="link" size="sm" onClick={() => setAll(!areAllSelected)}>
                    {t("toggleAll", { context: areAllSelected ? "deselect" : "select" })}
                </Button>
            </div>
            <Card className="p-4">
                {groups.map((group, index) => (
                    <React.Fragment key={index}>
                        {index > 0 && <hr className="my-1" />}
                        {group}
                    </React.Fragment>
                ))}
            </Card>
        </>
    );
}

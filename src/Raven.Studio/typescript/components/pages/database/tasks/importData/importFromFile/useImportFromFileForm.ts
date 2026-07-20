import { useEffect, useRef } from "react";
import { useForm, useWatch } from "react-hook-form";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import {
    DatabaseSettingKey,
    ImportFromFileFormData,
    importFromFileYupResolver,
    OngoingTaskKey,
} from "./importFromFileValidation";
import { getDefaultFormData } from "./importFromFileUtils";
import { DocumentToggleKey, useImportLicenseRestrictions } from "./useImportLicenseRestrictions";

export { getDefaultFormData };

export const defaultTransformScript =
    "this.collection = this['@metadata']['@collection'];\r\n" +
    "// current object is available under 'this' variable\r\n" +
    "// @change-vector, @id, @last-modified metadata fields are not available";

export function useImportFromFileForm() {
    const isAdminAccessOrAbove = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { restrictedFeatures, restrictedOngoingTasks, restrictedDocumentToggles } = useImportLicenseRestrictions();

    // License state normally lives in Redux before mount, so the defaults are deterministic:
    // license-restricted settings, ongoing tasks and document toggles start unchecked. If the
    // license status arrives (or changes) after mount, the effect below re-applies the gating.
    const defaults = getDefaultFormData(isAdminAccessOrAbove);
    restrictedFeatures.forEach(({ settingKey }) => {
        defaults.configuration.databaseSettings[settingKey] = false;
    });
    restrictedOngoingTasks.forEach(({ taskKey }) => {
        defaults.configuration.ongoingTasks[taskKey] = false;
    });
    restrictedDocumentToggles.forEach((toggleKey) => {
        defaults.documents[toggleKey] = false;
    });

    const form = useForm<ImportFromFileFormData>({
        resolver: importFromFileYupResolver,
        mode: "onChange",
        defaultValues: defaults,
    });

    const { control, setValue } = form;

    // If the license status transitions after mount (e.g. it wasn't loaded yet when the view was
    // deep-linked), re-apply the gating: newly restricted fields go off, newly allowed fields go
    // back to their defaults. Restricted inputs are disabled in the UI, so no user intent is lost.
    const prevRestricted = useRef<{
        settings: Set<DatabaseSettingKey>;
        tasks: Set<OngoingTaskKey>;
        toggles: Set<DocumentToggleKey>;
    } | null>(null);

    useEffect(() => {
        const current = {
            settings: new Set(restrictedFeatures.map((x) => x.settingKey)),
            tasks: new Set(restrictedOngoingTasks.map((x) => x.taskKey)),
            toggles: new Set(restrictedDocumentToggles),
        };
        const prev = prevRestricted.current;
        prevRestricted.current = current;
        if (!prev) {
            return; // first render - the gating is already baked into defaultValues
        }

        const baseDefaults = getDefaultFormData(isAdminAccessOrAbove);
        current.settings.forEach((settingKey) => {
            if (!prev.settings.has(settingKey)) {
                setValue(`configuration.databaseSettings.${settingKey}`, false);
            }
        });
        prev.settings.forEach((settingKey) => {
            if (!current.settings.has(settingKey)) {
                setValue(
                    `configuration.databaseSettings.${settingKey}`,
                    baseDefaults.configuration.databaseSettings[settingKey]
                );
            }
        });
        current.tasks.forEach((taskKey) => {
            if (!prev.tasks.has(taskKey)) {
                setValue(`configuration.ongoingTasks.${taskKey}`, false);
            }
        });
        prev.tasks.forEach((taskKey) => {
            if (!current.tasks.has(taskKey)) {
                setValue(`configuration.ongoingTasks.${taskKey}`, baseDefaults.configuration.ongoingTasks[taskKey]);
            }
        });
        current.toggles.forEach((toggleKey) => {
            if (!prev.toggles.has(toggleKey)) {
                setValue(`documents.${toggleKey}`, false);
            }
        });
        prev.toggles.forEach((toggleKey) => {
            if (!current.toggles.has(toggleKey)) {
                setValue(`documents.${toggleKey}`, baseDefaults.documents[toggleKey]);
            }
        });
    }, [restrictedFeatures, restrictedOngoingTasks, restrictedDocumentToggles, isAdminAccessOrAbove, setValue]);

    const documents = useWatch({ control, name: "documents" });
    const configuration = useWatch({ control, name: "configuration" });
    const isUseTransformScript = useWatch({ control, name: "processing.isUseTransformScript" });

    // Knockout parity: disabling documents forces attachments off (the reverse direction -
    // enabling counters/revisions/time series/attachments forcing documents on - is handled
    // via FormSwitch afterChange in DataToImportSection, matching Knockout's directional
    // subscriptions).
    useEffect(() => {
        if (!documents.isIncludeDocuments && documents.isIncludeAttachments) {
            setValue("documents.isIncludeAttachments", false);
        }
    }, [documents.isIncludeDocuments, documents.isIncludeAttachments, setValue]);

    // Knockout parity: disabling indexes forces analyzer-removal and index-history off (the
    // reverse direction is handled via FormSwitch afterChange in DataToImportSection).
    useEffect(() => {
        if (!configuration.isIncludeIndexes) {
            if (configuration.isRemoveAnalyzers) {
                setValue("configuration.isRemoveAnalyzers", false);
            }
            if (configuration.isIncludeIndexHistory) {
                setValue("configuration.isIncludeIndexHistory", false);
            }
        }
    }, [
        configuration.isIncludeIndexes,
        configuration.isRemoveAnalyzers,
        configuration.isIncludeIndexHistory,
        setValue,
    ]);

    useEffect(() => {
        if (isUseTransformScript) {
            setValue("processing.transformScript", defaultTransformScript);
        } else {
            setValue("processing.transformScript", "");
        }
    }, [isUseTransformScript, setValue]);

    return form;
}

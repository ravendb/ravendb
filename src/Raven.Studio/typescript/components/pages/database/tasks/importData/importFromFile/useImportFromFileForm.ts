import { useEffect, useRef } from "react";
import { useForm, useWatch } from "react-hook-form";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import {
    ConnectionStringKey,
    DatabaseSettingKey,
    ImportFromFileFormData,
    importFromFileYupResolver,
    OngoingTaskKey,
} from "./importFromFileValidation";
import { getDefaultFormData } from "./importFromFileUtils";
import { DocumentToggleKey, useImportRestrictions } from "./useImportRestrictions";

export { getDefaultFormData };

export const defaultTransformScript =
    "this.collection = this['@metadata']['@collection'];\r\n" +
    "// current object is available under 'this' variable\r\n" +
    "// @change-vector, @id, @last-modified metadata fields are not available";

export function useImportFromFileForm() {
    const isAdminAccessOrAbove = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const {
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys,
        restrictedDocumentToggleKeys,
        hasAnyRestriction,
    } = useImportRestrictions();

    // License state normally lives in Redux before mount, so the defaults are deterministic:
    // restricted settings, ongoing tasks, connection strings and document toggles start unchecked.
    // If the license status arrives (or changes) after mount, the effect below re-applies the gating.
    const defaults = getDefaultFormData(isAdminAccessOrAbove);
    restrictedSettingKeys.forEach((settingKey) => {
        defaults.configuration.databaseSettings[settingKey] = false;
    });
    restrictedOngoingTaskKeys.forEach((taskKey) => {
        defaults.configuration.ongoingTasks[taskKey] = false;
    });
    restrictedConnectionStringKeys.forEach((key) => {
        defaults.configuration.connectionStrings[key] = false;
    });
    restrictedDocumentToggleKeys.forEach((toggleKey) => {
        defaults.documents[toggleKey] = false;
    });

    // Restricted rows live inside the "Customize" panels, so expand them up front - otherwise the
    // user sees "Include Connection Strings & Ongoing Tasks" on with no hint that parts are off.
    if (hasAnyRestriction) {
        defaults.configuration.isCustomizeOngoingTasks = true;
        defaults.configuration.isImportAllSettings = false;
    }

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
        connectionStrings: Set<ConnectionStringKey>;
        toggles: Set<DocumentToggleKey>;
    } | null>(null);

    useEffect(() => {
        const current = {
            settings: new Set(restrictedSettingKeys),
            tasks: new Set(restrictedOngoingTaskKeys),
            connectionStrings: new Set(restrictedConnectionStringKeys),
            toggles: new Set(restrictedDocumentToggleKeys),
        };
        const prev = prevRestricted.current;
        prevRestricted.current = current;
        if (!prev) {
            return; // first render - the gating is already baked into defaultValues
        }

        const baseDefaults = getDefaultFormData(isAdminAccessOrAbove);
        const hasNewRestriction =
            [...current.settings].some((key) => !prev.settings.has(key)) ||
            [...current.tasks].some((key) => !prev.tasks.has(key)) ||
            [...current.connectionStrings].some((key) => !prev.connectionStrings.has(key)) ||
            [...current.toggles].some((key) => !prev.toggles.has(key));
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
        current.connectionStrings.forEach((key) => {
            if (!prev.connectionStrings.has(key)) {
                setValue(`configuration.connectionStrings.${key}`, false);
            }
        });
        prev.connectionStrings.forEach((key) => {
            if (!current.connectionStrings.has(key)) {
                setValue(`configuration.connectionStrings.${key}`, baseDefaults.configuration.connectionStrings[key]);
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

        // Newly gated rows sit inside the "Customize" panels, so expand them the same way the
        // mount-time defaults do - otherwise the rows switch off behind a collapsed panel.
        // Gated on an actual diff: the license status object gets a fresh identity on unrelated
        // store updates (e.g. cluster topology notifications), and re-applying the expansion
        // unconditionally would silently revert the user's customize choices.
        if (hasNewRestriction) {
            setValue("configuration.isCustomizeOngoingTasks", true);
            setValue("configuration.isImportAllSettings", false);
        }
    }, [
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys,
        restrictedDocumentToggleKeys,
        isAdminAccessOrAbove,
        setValue,
    ]);

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

import { useEffect, useRef } from "react";
import { useForm, useWatch } from "react-hook-form";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import {
    ConnectionStringKey,
    DatabaseSettingKey,
    DocumentToggleKey,
    ImportFromFileFormData,
    importFromFileYupResolver,
    OngoingTaskKey,
} from "./importFromFileValidation";
import { getDefaultFormData } from "./importFromFileUtils";
import { useImportRestrictions } from "./useImportRestrictions";

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

    useEffect(() => {
        if (!documents.isIncludeDocuments) {
            setValue("documents.isIncludeAttachments", false);
        }
    }, [documents.isIncludeDocuments, setValue]);

    useEffect(() => {
        if (!configuration.isIncludeIndexes) {
            setValue("configuration.isRemoveAnalyzers", false);
            setValue("configuration.isIncludeIndexHistory", false);
        }
    }, [configuration.isIncludeIndexes, setValue]);

    useEffect(() => {
        if (isUseTransformScript) {
            setValue("processing.transformScript", defaultTransformScript);
        } else {
            setValue("processing.transformScript", "");
        }
    }, [isUseTransformScript, setValue]);

    return form;
}

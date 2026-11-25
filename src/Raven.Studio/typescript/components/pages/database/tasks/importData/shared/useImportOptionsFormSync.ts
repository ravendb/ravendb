import { useEffect, useRef } from "react";
import { UseFormReturn, useWatch } from "react-hook-form";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import {
    ConnectionStringKey,
    DatabaseSettingKey,
    DocumentToggleKey,
    ImportOptionsFormData,
    OngoingTaskKey,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import {
    ImportRestrictions,
    useImportRestrictions,
} from "components/pages/database/tasks/importData/shared/useImportRestrictions";

export const defaultTransformScript =
    "this.collection = this['@metadata']['@collection'];\r\n" +
    "// current object is available under 'this' variable\r\n" +
    "// @change-vector, @id, @last-modified metadata fields are not available";

type GetDefaultImportOptions = (isAdminAccessOrAbove: boolean) => ImportOptionsFormData;

export function withRestrictedDefaults<TFormData extends ImportOptionsFormData>(
    defaults: TFormData,
    restrictions: ImportRestrictions
): TFormData {
    restrictions.restrictedSettingKeys.forEach((settingKey) => {
        defaults.configuration.databaseSettings[settingKey] = false;
    });
    restrictions.restrictedOngoingTaskKeys.forEach((taskKey) => {
        defaults.configuration.ongoingTasks[taskKey] = false;
    });
    restrictions.restrictedConnectionStringKeys.forEach((key) => {
        defaults.configuration.connectionStrings[key] = false;
    });
    restrictions.restrictedDocumentToggleKeys.forEach((toggleKey) => {
        defaults.documents[toggleKey] = false;
    });

    if (restrictions.allRestrictedItems.length > 0) {
        defaults.configuration.isCustomizeOngoingTasks = true;
        defaults.configuration.isImportAllSettings = false;
    }

    return defaults;
}

export function useImportOptionsFormSync(
    { control, setValue }: UseFormReturn<ImportOptionsFormData>,
    getDefaultImportOptions: GetDefaultImportOptions
) {
    const documents = useWatch({ control, name: "documents" });
    const configuration = useWatch({ control, name: "configuration" });
    const isUseTransformScript = useWatch({ control, name: "processing.isUseTransformScript" });

    useRestrictionsFormSync(setValue, getDefaultImportOptions);

    // attachments cannot be imported without the documents they belong to
    useEffect(() => {
        if (!documents.isIncludeDocuments) {
            setValue("documents.isIncludeAttachments", false);
        }
    }, [documents.isIncludeDocuments, setValue]);

    // analyzers and index history only make sense when indexes are imported
    useEffect(() => {
        if (!configuration.isIncludeIndexes) {
            setValue("configuration.isRemoveAnalyzers", false);
            setValue("configuration.isIncludeIndexHistory", false);
        }
    }, [configuration.isIncludeIndexes, setValue]);

    // the script field shows a starter template when enabled and is cleared when disabled
    useEffect(() => {
        if (isUseTransformScript) {
            setValue("processing.transformScript", defaultTransformScript);
        } else {
            setValue("processing.transformScript", "");
        }
    }, [isUseTransformScript, setValue]);
}

function useRestrictionsFormSync(
    setValue: UseFormReturn<ImportOptionsFormData>["setValue"],
    getDefaultImportOptions: GetDefaultImportOptions
) {
    const isAdminAccessOrAbove = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const {
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys,
        restrictedDocumentToggleKeys,
    } = useImportRestrictions();

    const prevRestricted = useRef<{
        settings: Set<DatabaseSettingKey>;
        tasks: Set<OngoingTaskKey>;
        connectionStrings: Set<ConnectionStringKey>;
        toggles: Set<DocumentToggleKey>;
    } | null>(null);

    // restrictions can change after mount (license, access, source version), so the affected fields follow them
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

        const baseDefaults = getDefaultImportOptions(isAdminAccessOrAbove);
        const hasNewConfigurationRestriction =
            [...current.settings].some((key) => !prev.settings.has(key)) ||
            [...current.tasks].some((key) => !prev.tasks.has(key)) ||
            [...current.connectionStrings].some((key) => !prev.connectionStrings.has(key));
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

        if (hasNewConfigurationRestriction) {
            setValue("configuration.isCustomizeOngoingTasks", true);
            setValue("configuration.isImportAllSettings", false);
        }
    }, [
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys,
        restrictedDocumentToggleKeys,
        isAdminAccessOrAbove,
        getDefaultImportOptions,
        setValue,
    ]);
}

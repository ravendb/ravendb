import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Form from "react-bootstrap/Form";
import RichAlert from "components/common/RichAlert";
import { DatabaseSettingKey, ImportFromFileFormData, databaseSettingKeys } from "../importFromFileValidation";
import { useMinPeriodWarning } from "../useMinPeriodWarning";
import { useImportRestrictions } from "../useImportRestrictions";

/**
 * Warns that importing Document Refresh / Expiration can fail the whole import: the server enforces
 * a minimum frequency, and the frequency lives inside the dump, which the Studio never reads. So
 * unlike the neighbouring restriction alerts this one cannot be resolved up front - it offers a
 * switch per affected setting to drop it from the import.
 */
export default function MinPeriodWarningAlert() {
    const { control, setValue } = useFormContext<ImportFromFileFormData>();
    const { databaseSettings: settingRestrictions } = useImportRestrictions();
    const warnings = useMinPeriodWarning();

    // No file picked yet means nothing to exclude from: the configuration fieldset is disabled at
    // that point, so toggling here would silently change values the user cannot see.
    const file = useWatch({ control, name: "file" });
    const isImportAllSettings = useWatch({ control, name: "configuration.isImportAllSettings" });
    const databaseSettings = useWatch({ control, name: "configuration.databaseSettings" });

    if (warnings.length === 0) {
        return null;
    }

    const setSettingIncluded = (keyToSet: DatabaseSettingKey, isIncluded: boolean) => {
        if (!isImportAllSettings) {
            setValue(`configuration.databaseSettings.${keyToSet}`, isIncluded, { shouldDirty: true });
            return;
        }

        // "Import all settings" would keep re-including the row, so switch to the customize view
        // and materialise what "all" meant: every non-gated setting on, this one as requested.
        setValue("configuration.isImportAllSettings", false, { shouldDirty: true });
        databaseSettingKeys.forEach((key) => {
            if (settingRestrictions[key]) {
                return; // gated for another reason - its default stays false
            }
            setValue(`configuration.databaseSettings.${key}`, key === keyToSet ? isIncluded : true, {
                shouldDirty: true,
            });
        });
    };

    return (
        <RichAlert variant="warning" title="Frequency limited by your license" className="mt-3 mb-0">
            <div>
                Your license limits the frequency of the following settings. If this file carries a shorter
                frequency, the whole import will fail - turn a setting off to leave it out of the import.
            </div>
            <div className="d-flex flex-column gap-1 mt-2">
                {warnings.map(({ key, label, minPeriodInHours }) => (
                    <Form.Check
                        key={key}
                        type="switch"
                        id={`min-period-${key}`}
                        className="m-0"
                        disabled={!file}
                        // "Import all settings" includes the row regardless of its own field value
                        checked={isImportAllSettings || !!databaseSettings?.[key]}
                        onChange={(e) => setSettingIncluded(key, e.target.checked)}
                        label={`${label} (min. ${minPeriodInHours} hours)`}
                    />
                ))}
            </div>
        </RichAlert>
    );
}

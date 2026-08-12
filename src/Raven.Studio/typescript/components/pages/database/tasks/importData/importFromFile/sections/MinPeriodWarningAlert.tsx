import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { DatabaseSettingKey, ImportFromFileFormData, databaseSettingKeys } from "../importFromFileValidation";
import { useMinPeriodWarning } from "../useMinPeriodWarning";
import { useImportRestrictions } from "../useImportRestrictions";

export default function MinPeriodWarningAlert() {
    const { control, setValue } = useFormContext<ImportFromFileFormData>();
    const { databaseSettings: settingRestrictions } = useImportRestrictions();
    const warnings = useMinPeriodWarning();

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
        <div className="import-info-panel mt-3">
            <h4 className="hstack gap-1 mb-1">
                <Icon icon="license" margin="m-0" /> Frequency limited by your license
            </h4>
            <div>
                Your license limits the frequency of the following settings. If this file carries a shorter frequency,
                the whole import will fail - turn a setting off to leave it out of the import.
            </div>
            <div className="d-flex flex-column gap-1 mt-2">
                {warnings.map(({ key, label, minPeriodInHours }) => (
                    <Form.Check
                        key={key}
                        type="switch"
                        id={`min-period-${key}`}
                        className="m-0"
                        disabled={!file}
                        checked={isImportAllSettings || !!databaseSettings?.[key]}
                        onChange={(e) => setSettingIncluded(key, e.target.checked)}
                        label={`${label} (min. ${minPeriodInHours} hours)`}
                    />
                ))}
            </div>
        </div>
    );
}

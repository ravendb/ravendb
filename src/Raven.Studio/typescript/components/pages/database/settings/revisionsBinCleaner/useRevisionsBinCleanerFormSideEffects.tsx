import { UseFormSetValue, UseFormWatch } from "react-hook-form";
import { RevisionsBinCleanerFormData } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import { useEffect } from "react";

export default function useRevisionsBinCleanerFormSideEffects(
    watch: UseFormWatch<RevisionsBinCleanerFormData>,
    setValue: UseFormSetValue<RevisionsBinCleanerFormData>
) {
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            switch (name) {
                case "isRevisionsBinCleanerEnabled": {
                    if (!values.isRevisionsBinCleanerEnabled) {
                        setValue("isMinimumEntriesAgeToKeepEnabled", false, { shouldValidate: true });
                        setValue("isRefreshFrequencyEnabled", false, { shouldValidate: true });
                    }
                    break;
                }
                case "isMinimumEntriesAgeToKeepEnabled": {
                    if (!values.isMinimumEntriesAgeToKeepEnabled) {
                        setValue("minimumEntriesAgeToKeepInMin", null, { shouldValidate: true });
                    }
                    break;
                }
                case "isRefreshFrequencyEnabled": {
                    if (!values.isRefreshFrequencyEnabled) {
                        setValue("refreshFrequencyInSec", null, { shouldValidate: true });
                    }
                }
            }
        });
        return () => unsubscribe();
    }, [setValue, watch]);
}

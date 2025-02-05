import { UseFormSetValue, UseFormWatch } from "react-hook-form";
import { RevisionsBinCleanerFormData } from "components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleanerValidation";
import { useEffect } from "react";
import moment from "moment";

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
                        setValue("minimumEntriesAgeToKeep", null, { shouldValidate: true });
                    } else {
                        if (values.minimumEntriesAgeToKeep === null) {
                            setValue("minimumEntriesAgeToKeep", moment.duration(1, "month").asSeconds());
                        }
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

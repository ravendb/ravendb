import { Control, UseFormSetValue, useWatch } from "react-hook-form";
import { EditDocumentRevisionsCollectionConfig } from "./DocumentRevisionsValidation";
import { useEffect } from "react";

export default function useEditRevisionFormController(
    control: Control<EditDocumentRevisionsCollectionConfig>,
    setValue: UseFormSetValue<EditDocumentRevisionsCollectionConfig>
) {
    const formValues = useWatch({ control: control });

    useEffect(() => {
        if (!formValues.IsMinimumRevisionAgeToKeepEnabled && formValues.MinimumRevisionAgeToKeep !== null) {
            setValue("MinimumRevisionAgeToKeep", null, { shouldValidate: true });
        }

        if (!formValues.IsMinimumRevisionsToKeepEnabled && formValues.MinimumRevisionsToKeep !== null) {
            setValue("MinimumRevisionsToKeep", null, { shouldValidate: true });
        }

        if (
            !formValues.IsMinimumRevisionAgeToKeepEnabled &&
            !formValues.IsMinimumRevisionsToKeepEnabled &&
            formValues.IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled
        ) {
            setValue("IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled", false, { shouldValidate: true });
        }

        if (
            !formValues.IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
            formValues.MaximumRevisionsToDeleteUponDocumentUpdate !== null
        ) {
            setValue("MaximumRevisionsToDeleteUponDocumentUpdate", null, { shouldValidate: true });
        }
    }, [
        formValues.IsMaximumRevisionsToDeleteUponDocumentUpdateEnabled,
        formValues.IsMinimumRevisionAgeToKeepEnabled,
        formValues.IsMinimumRevisionsToKeepEnabled,
        formValues.MaximumRevisionsToDeleteUponDocumentUpdate,
        formValues.MinimumRevisionAgeToKeep,
        formValues.MinimumRevisionsToKeep,
        setValue,
    ]);

    return formValues;
}

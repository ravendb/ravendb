import { Control, UseFormSetValue, useWatch } from "react-hook-form";
import { EditDocumentRevisionsCollectionConfig } from "./DocumentRevisionsValidation";
import { useEffect } from "react";

export default function useEditRevisionFormController(
    control: Control<EditDocumentRevisionsCollectionConfig>,
    setValue: UseFormSetValue<EditDocumentRevisionsCollectionConfig>
) {
    const formValues = useWatch({ control: control });

    useEffect(() => {
        if (!formValues.isMinimumRevisionAgeToKeepEnabled && formValues.minimumRevisionAgeToKeep !== null) {
            setValue("minimumRevisionAgeToKeep", null, { shouldValidate: true });
        }

        if (!formValues.isMinimumRevisionsToKeepEnabled && formValues.minimumRevisionsToKeep !== null) {
            setValue("minimumRevisionsToKeep", null, { shouldValidate: true });
        }

        if (
            !formValues.isMinimumRevisionAgeToKeepEnabled &&
            !formValues.isMinimumRevisionsToKeepEnabled &&
            formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled
        ) {
            setValue("isMaximumRevisionsToDeleteUponDocumentUpdateEnabled", false, { shouldValidate: true });
        }

        if (
            !formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
            formValues.maximumRevisionsToDeleteUponDocumentUpdate !== null
        ) {
            setValue("maximumRevisionsToDeleteUponDocumentUpdate", null, { shouldValidate: true });
        }
    }, [
        formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled,
        formValues.isMinimumRevisionAgeToKeepEnabled,
        formValues.isMinimumRevisionsToKeepEnabled,
        formValues.maximumRevisionsToDeleteUponDocumentUpdate,
        formValues.minimumRevisionAgeToKeep,
        formValues.minimumRevisionsToKeep,
        setValue,
    ]);

    return formValues;
}

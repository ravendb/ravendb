import { useEffect } from "react";
import { UseFormReturn, useWatch } from "react-hook-form";
import { ImportOptionsFormData } from "components/pages/database/tasks/importData/shared/importOptionsValidation";

export const defaultTransformScript =
    "this.collection = this['@metadata']['@collection'];\r\n" +
    "// current object is available under 'this' variable\r\n" +
    "// @change-vector, @id, @last-modified metadata fields are not available";

export function useImportOptionsFormSync({ control, setValue }: UseFormReturn<ImportOptionsFormData>) {
    const documents = useWatch({ control, name: "documents" });
    const configuration = useWatch({ control, name: "configuration" });
    const isUseTransformScript = useWatch({ control, name: "processing.isUseTransformScript" });

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

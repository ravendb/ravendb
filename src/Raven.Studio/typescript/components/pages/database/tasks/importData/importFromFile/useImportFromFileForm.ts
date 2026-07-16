import { useEffect } from "react";
import { useForm, useWatch } from "react-hook-form";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ImportFromFileFormData, importFromFileYupResolver } from "./importFromFileValidation";
import { getDefaultFormData } from "./importFromFileUtils";

export { getDefaultFormData };

export const defaultTransformScript =
    "this.collection = this['@metadata']['@collection'];\r\n" +
    "// current object is available under 'this' variable\r\n" +
    "// @change-vector, @id, @last-modified metadata fields are not available";

export function useImportFromFileForm() {
    const isAdminAccessOrAbove = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const form = useForm<ImportFromFileFormData>({
        resolver: importFromFileYupResolver,
        mode: "onChange",
        defaultValues: getDefaultFormData(isAdminAccessOrAbove),
    });

    const { control, setValue } = form;

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

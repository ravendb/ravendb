import { useForm } from "react-hook-form";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ImportFromFileFormData, importFromFileYupResolver } from "./importFromFileValidation";
import { getDefaultFormData } from "./importFromFileUtils";
import { useImportRestrictions } from "components/pages/database/tasks/importData/shared/useImportRestrictions";
import {
    defaultTransformScript,
    useImportOptionsFormSync,
    withRestrictedDefaults,
} from "components/pages/database/tasks/importData/shared/useImportOptionsFormSync";

export { getDefaultFormData, defaultTransformScript };

export function useImportFromFileForm() {
    const isAdminAccessOrAbove = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const restrictions = useImportRestrictions();

    const form = useForm<ImportFromFileFormData>({
        resolver: importFromFileYupResolver,
        mode: "onChange",
        defaultValues: withRestrictedDefaults(getDefaultFormData(isAdminAccessOrAbove), restrictions),
    });

    useImportOptionsFormSync(form, getDefaultFormData);

    return form;
}

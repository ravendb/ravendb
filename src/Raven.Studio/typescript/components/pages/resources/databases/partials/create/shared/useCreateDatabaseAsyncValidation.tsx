import { useServices } from "components/hooks/useServices";
import { UseFormSetError } from "react-hook-form";
import { CreateDatabaseRegularFormData } from "../regular/createDatabaseRegularValidation";
import { CreateDatabaseFromBackupFormData } from "../formBackup/createDatabaseFromBackupValidation";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";

export const useCreateDatabaseAsyncValidation = (
    databaseName: string,
    setError: UseFormSetError<CreateDatabaseRegularFormData | CreateDatabaseFromBackupFormData>
) => {
    const { resourcesService } = useServices();

    return useAsyncDebounce(
        async (databaseName) => {
            if (!databaseName) {
                return;
            }

            const result = await resourcesService.validateName("Database", databaseName);
            if (!result.IsValid) {
                setError("basicInfoStep.databaseName", {
                    type: "manual",
                    message: result.ErrorMessage,
                });
            }

            return result.IsValid;
        },
        [databaseName]
    );
};

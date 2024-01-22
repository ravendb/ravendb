import { useServices } from "components/hooks/useServices";
import { useEffect } from "react";
import { UseFormSetError, UseFormClearErrors } from "react-hook-form";

interface FormData {
    databaseName: string;
}

export const useDatabaseNameValidation = (
    databaseName: string,
    setError: UseFormSetError<FormData>,
    clearErrors: UseFormClearErrors<FormData>
) => {
    const { resourcesService } = useServices();

    useEffect(() => {
        if (!databaseName) {
            return;
        }

        debouncedValidateName(() => resourcesService.validateName("Database", databaseName), setError, clearErrors);
    }, [databaseName, resourcesService, setError, clearErrors]);
};

const debouncedValidateName = _.debounce(
    async (
        validateName: () => Promise<Raven.Client.Util.NameValidation>,
        setError: UseFormSetError<FormData>,
        clearErrors: UseFormClearErrors<FormData>
    ) => {
        const result = await validateName();

        if (result.IsValid) {
            clearErrors("databaseName");
        } else {
            setError("databaseName", {
                type: "manual",
                message: result.ErrorMessage,
            });
        }
    },
    500
);

import { useServices } from "components/hooks/useServices";
import { useEffect, useState } from "react";
import { UseFormSetError } from "react-hook-form";

interface FormData {
    basicInfo: {
        databaseName: string;
    };
}

type ValidationResult = "valid" | "loading" | "error";

export const useCreateDatabaseAsyncValidation = (databaseName: string, setError: UseFormSetError<FormData>) => {
    const [result, setResult] = useState<ValidationResult>("loading");
    const { resourcesService } = useServices();

    useEffect(() => {
        if (!databaseName) {
            return;
        }

        debouncedValidate(() => resourcesService.validateName("Database", databaseName), setError, setResult);
    }, [databaseName, resourcesService, setError]);

    return result;
};

const debouncedValidate = _.debounce(
    async (
        validateName: () => Promise<Raven.Client.Util.NameValidation>,
        setError: UseFormSetError<FormData>,
        setResult: (result: ValidationResult) => void
    ) => {
        setResult("loading");
        const result = await validateName();

        if (result.IsValid) {
            setResult("valid");
        } else {
            setError("basicInfo.databaseName", {
                type: "manual",
                message: result.ErrorMessage,
            });
            setResult("error");
        }
    },
    500
);

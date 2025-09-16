import { Icon } from "components/common/Icon";
import { useFormContext } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { useEffect } from "react";

interface EditAiAgentErrorIconProps {
    fieldNames: (keyof EditAiAgentFormData)[];
    openPanel: (value: boolean) => void;
}

export default function EditAiAgentErrorIcon({ fieldNames, openPanel }: EditAiAgentErrorIconProps) {
    const {
        formState: { errors },
    } = useFormContext<EditAiAgentFormData>();

    const hasErrors = fieldNames.some((fieldName) => errors[fieldName]);

    useEffect(() => {
        if (hasErrors) {
            openPanel(true);
        }
    }, [hasErrors]);

    if (!hasErrors) {
        return null;
    }

    return <Icon icon="warning" color="danger" margin="ms-1" />;
}

import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { FormLabel } from "components/common/Form";
import ClickableCard from "components/common/ClickableCard";

type FormData = ConnectionFormData<AiConnection>;

export default function ModelTypeField() {
    const { control, setValue } = useFormContext<FormData>();

    const formValues = useWatch({ control });

    return (
        <div className="mb-2">
            <FormLabel>
                Model type
                <PopoverWithHoverWrapper message="Select the type of model this connection will target">
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <div className="d-flex gap-2">
                <ClickableCard
                    icon="llm"
                    title="Chat"
                    description="Conversational AI and content generation model"
                    className="w-50"
                    isSelected={formValues.modelType === "Chat"}
                    onClick={() => setValue("modelType", "Chat")}
                />
                <ClickableCard
                    icon="document2"
                    title="Text embeddings"
                    description="Embedding generation model for vector search and similarity comparison"
                    className="w-50"
                    isSelected={formValues.modelType === "TextEmbeddings"}
                    onClick={() => setValue("modelType", "TextEmbeddings")}
                />
            </div>
        </div>
    );
}

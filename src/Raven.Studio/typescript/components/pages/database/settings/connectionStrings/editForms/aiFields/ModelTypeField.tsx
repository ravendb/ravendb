import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { FormLabel } from "components/common/Form";
import ClickableCard from "components/common/ClickableCard";
import { useAppSelector } from "components/store";
import { connectionStringSelectors } from "../../store/connectionStringsSlice";

type FormData = ConnectionFormData<AiConnection>;

interface ModelTypeFieldProps {
    initialModelType: FormData["modelType"];
}

export default function ModelTypeField({ initialModelType }: ModelTypeFieldProps) {
    const { control, setValue } = useFormContext<FormData>();

    const formValues = useWatch({ control });

    const viewContext = useAppSelector(connectionStringSelectors.viewContext);

    const isChatVisible = viewContext !== "aiTask" || initialModelType !== "TextEmbeddings";
    const isTextEmbeddingsVisible = viewContext !== "aiTask" || initialModelType !== "Chat";
    const isAllVisible = isChatVisible && isTextEmbeddingsVisible;

    return (
        <div className="mb-2">
            <FormLabel>
                Model type
                <PopoverWithHoverWrapper message="Select the type of model this connection will target">
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <div className="d-flex gap-2">
                {isChatVisible && (
                    <ClickableCard
                        icon="llm"
                        title="Chat"
                        description="Conversational AI and content generation model"
                        className={isAllVisible ? "w-50" : "w-100"}
                        isSelected={formValues.modelType === "Chat"}
                        onClick={() => setValue("modelType", "Chat")}
                    />
                )}
                {isTextEmbeddingsVisible && (
                    <ClickableCard
                        icon="document2"
                        title="Text embeddings"
                        description="Embedding generation model for vector search and similarity comparison"
                        className={isAllVisible ? "w-50" : "w-100"}
                        isSelected={formValues.modelType === "TextEmbeddings"}
                        onClick={() => setValue("modelType", "TextEmbeddings")}
                    />
                )}
            </div>
        </div>
    );
}

import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import classNames from "classnames";
import IconName from "typings/server/icons";
import { FormLabel } from "components/common/Form";

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

interface ClickableCardProps {
    icon: IconName;
    title: string;
    description: string;
    isSelected: boolean;
    className?: string;
    onClick: () => void;
}

function ClickableCard({ description, icon, onClick, title, isSelected, className }: ClickableCardProps) {
    return (
        <div
            className={classNames(
                "border rounded p-2 cursor-pointer",
                {
                    "bg-faded-primary border-primary": isSelected,
                },
                {
                    "border-secondary": !isSelected,
                },
                className
            )}
            onClick={onClick}
        >
            <div className="text-emphasis hstack gap-2">
                <div>
                    <Icon icon={icon} margin="mx-1" />
                </div>
                <div className="flex-grow">
                    <div className="fw-semibold">{title}</div>
                    <div>{description}</div>
                </div>
            </div>
        </div>
    );
}

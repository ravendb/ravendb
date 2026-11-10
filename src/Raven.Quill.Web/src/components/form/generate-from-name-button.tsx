import { RefreshCw } from "lucide-react";
import { ButtonWithDisabledReason } from "@/components/button-with-disabled-reason";

interface GenerateFromNameButtonProps {
    isAlreadyGenerated: boolean;
    alreadyGeneratedTooltip: string;
    isBusy?: boolean;
    onGenerate: () => void;
}

export function GenerateFromNameButton({
    isAlreadyGenerated,
    alreadyGeneratedTooltip,
    isBusy = false,
    onGenerate,
}: GenerateFromNameButtonProps) {
    return (
        <ButtonWithDisabledReason
            variant="ghost"
            disabledReason={isAlreadyGenerated ? alreadyGeneratedTooltip : ""}
            disabled={isBusy}
            className="aria-disabled:hover:bg-transparent"
            onClick={onGenerate}
        >
            <RefreshCw />
            Generate from name
        </ButtonWithDisabledReason>
    );
}

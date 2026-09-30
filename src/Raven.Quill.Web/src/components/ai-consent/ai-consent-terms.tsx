import { Checkbox } from "@/components/shadcn/ui/checkbox";

const AI_TERMS_OF_USE_URL = "https://ravendb.net/legal/ravendb/ai-assistant-terms-of-use";

type AiConsentTermsCheckboxProps = {
    isAccepted: boolean;
    onAcceptedChange: (isAccepted: boolean) => void;
    disabled?: boolean;
};

export function AiConsentTermsCheckbox({ isAccepted, onAcceptedChange, disabled }: AiConsentTermsCheckboxProps) {
    return (
        <label className="flex items-start gap-2 text-left text-sm">
            <Checkbox
                checked={isAccepted}
                onCheckedChange={(value) => onAcceptedChange(value === true)}
                disabled={disabled}
                className="mt-0.5"
            />
            <span>
                I accept the{" "}
                <a href={AI_TERMS_OF_USE_URL} target="_blank" rel="noreferrer" className="text-primary hover:underline">
                    RavenDB AI Assistant Terms of Use
                </a>
            </span>
        </label>
    );
}

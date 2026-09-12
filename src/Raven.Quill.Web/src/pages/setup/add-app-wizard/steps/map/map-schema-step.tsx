import { useId, useState } from "react";
import { Plus, Sparkles, Trash2Icon } from "lucide-react";
import { useFormContext } from "react-hook-form";
import { AiConsentGate, type AiConsentCopy } from "@/components/ai-consent/ai-consent-gate";
import { useAiConsent } from "@/components/ai-consent/use-ai-consent";
import { FormRadioCards } from "@/components/form/form-radio-cards";
import { FormTextarea } from "@/components/form/form-textarea";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { FieldLabel } from "@/components/shadcn/ui/field";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { AI_SUGGEST_OPTION, MANUAL_OPTION } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";

const MAP_CONSENT_COPY: AiConsentCopy = {
    gateDescription:
        "AI Suggest sends your schema and your intent prompt to the RavenDB AI service. Accept its Terms of Use to " +
        "unlock the option below - or map the schema yourself with Manual.",
    dialogTitle: "Map your schema with AI",
    dialogDescription:
        "AI reads your schema and works out the whole mapping, which you can review and edit in the next step. That " +
        "sends your schema to the RavenDB AI service, so it is available only once you accept its Terms of Use.",
};

export function MapSchemaStep({ isBusy }: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();
    const { isBlocked: isAiBlocked, unavailableReason } = useAiConsent();

    return (
        <div className="grid gap-5">
            {/* Above the cards, not inside the one it speaks for: that card is dimmed, and an
                acceptance the operator cannot read is worse than none. */}
            <AiConsentGate variant="banner" copy={MAP_CONSENT_COPY} />
            <FormRadioCards
                control={control}
                name="map.source"
                disabled={isBusy}
                className="gap-4"
                options={[
                    {
                        value: AI_SUGGEST_OPTION.value,
                        label: AI_SUGGEST_OPTION.label,
                        description: AI_SUGGEST_OPTION.description,
                        icon: <Sparkles className="size-5" aria-hidden="true" />,
                        // Disabled rather than hidden, so the operator can weigh it against Manual.
                        disabled: isAiBlocked,
                        badge: isAiBlocked && (
                            <Badge variant="secondary">{unavailableReason ? "Unavailable" : "Needs consent"}</Badge>
                        ),
                        content: ({ select }) => <IntentPrompt isDisabled={isBusy || isAiBlocked} select={select} />,
                    },
                    {
                        value: MANUAL_OPTION.value,
                        label: MANUAL_OPTION.label,
                        description: MANUAL_OPTION.description,
                    },
                ]}
            />
        </div>
    );
}

/**
 * The prompt steers the AI suggestion, but the step's actual question is AI versus manual, and the
 * suggestion works unaided. Inline the field read as the thing to fill in, inviting experiments that
 * derail the default mapping - so the two halves swap instead: a button that adds the field, and a
 * Remove that takes it away again. Deliberately not a collapse, which would imply the text keeps
 * steering the suggestion while hidden. Already-typed prompts open on the field.
 */
function IntentPrompt({ isDisabled, select }: { isDisabled: boolean; select: () => void }) {
    const { control, getValues, setValue } = useFormContext<AppFormData>();
    const promptId = useId();
    const [isShown, setIsShown] = useState(() => getValues("map.aiPrompt").trim().length > 0);
    // A prompt carried in from an earlier visit must not grab focus from the step; only swapping the
    // two halves by click has earned it, in either direction.
    const [shouldFocus, setShouldFocus] = useState(false);

    if (!isShown) {
        return (
            <Button
                variant="outline"
                size="sm"
                disabled={isDisabled}
                autoFocus={shouldFocus}
                // The prompt belongs to this card, so adding one also picks it.
                onClick={() => {
                    select();
                    setIsShown(true);
                    setShouldFocus(true);
                }}
            >
                <Plus aria-hidden="true" />
                Add an intent prompt
            </Button>
        );
    }

    return (
        <div className="grid animate-in gap-2 duration-200 fade-in-0 slide-in-from-top-1">
            <div className="flex items-center justify-between gap-2">
                <FieldLabel htmlFor={promptId}>Intent prompt</FieldLabel>
                <Button
                    variant="destructive"
                    size="xs"
                    disabled={isDisabled}
                    // Removing means going back to letting the AI decide, so the text goes too - it
                    // would otherwise keep steering the suggestion from a field nobody can see.
                    onClick={() => {
                        setValue("map.aiPrompt", "");
                        setIsShown(false);
                        setShouldFocus(true);
                    }}
                >
                    <Trash2Icon className="size-3" aria-hidden="true" />
                    Remove
                </Button>
            </div>
            <FormTextarea
                id={promptId}
                control={control}
                name="map.aiPrompt"
                placeholder='e.g. "Embed line items in each order, link customers by id, flatten addresses."'
                rows={4}
                disabled={isDisabled}
                onFocus={select}
                autoFocus={shouldFocus}
            />
        </div>
    );
}

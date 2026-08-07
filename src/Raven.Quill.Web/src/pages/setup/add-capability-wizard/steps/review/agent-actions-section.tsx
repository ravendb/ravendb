import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { Plus, Webhook } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import {
    DEFAULT_ACTION_RESPONSE_BYTES,
    type AgentFormData,
} from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { emptyAgentAction } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { ConfigItemCard, ConfigListEmpty } from "@/pages/setup/add-capability-wizard/steps/review/config-item-card";
import { SampleObjectAndSchemaTabs } from "@/pages/setup/add-capability-wizard/steps/review/sample-object-and-schema-tabs";
import { cn } from "@/lib/utils";

const DESCRIPTION_PLACEHOLDER =
    "Explain to the LLM when it should trigger this action. " +
    "E.g.: Use this action to open a support ticket once the customer confirms the details.";

export function AgentActionsSection({ className }: { className?: string }) {
    const { control } = useFormContext<AgentFormData>();
    const fieldArray = useFieldArray({ control, name: "review.actions" });

    return (
        <div className={cn("grid gap-3 rounded-lg border bg-background p-4", className)}>
            <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                    <Webhook className="size-4 text-muted-foreground" />
                    <span className="text-sm font-medium">Actions</span>
                </div>
                <Button variant="outline" size="sm" onClick={() => fieldArray.append(emptyAgentAction())}>
                    <Plus />
                    Add new action
                </Button>
            </div>

            {fieldArray.fields.length === 0 ? (
                <ConfigListEmpty label="No actions have been defined yet" />
            ) : (
                <div className="grid gap-2">
                    {fieldArray.fields.map((field, index) => (
                        <ActionItem key={field.id} index={index} remove={() => fieldArray.remove(index)} />
                    ))}
                </div>
            )}
        </div>
    );
}

function ActionItem({ index, remove }: { index: number; remove: () => void }) {
    const { control, setValue } = useFormContext<AgentFormData>();
    const action = useWatch({ control, name: `review.actions.${index}` });

    if (!action) {
        return null;
    }

    return (
        <ConfigItemCard
            isExpanded={action.isExpanded}
            editTitle="Configure action"
            summary={
                <>
                    <p className="truncate text-sm font-medium">{action.name || "(unnamed)"}</p>
                    {action.url && <p className="mt-0.5 truncate text-xs text-muted-foreground">{action.url}</p>}
                </>
            }
            onToggleExpanded={(isExpanded) => setValue(`review.actions.${index}.isExpanded`, isExpanded)}
            onRemove={remove}
        >
            <FormInput
                control={control}
                name={`review.actions.${index}.name`}
                label="Action name"
                placeholder="e.g. create_ticket"
            />
            <FormTextarea
                control={control}
                name={`review.actions.${index}.description`}
                label="Description"
                placeholder={DESCRIPTION_PLACEHOLDER}
                rows={3}
            />
            <SampleObjectAndSchemaTabs
                sampleObject={{
                    name: `review.actions.${index}.parametersSampleObject`,
                    label: "Sample parameters object",
                    placeholder: `{\n    // "ParamName": "Instruction to the LLM"\n}`,
                    description:
                        "A JSON object defining the parameters the LLM should supply when it triggers this action.",
                }}
                schema={{
                    name: `review.actions.${index}.parametersSchema`,
                    label: "Parameters JSON schema",
                    placeholder: `{\n    "type": "object",\n    "properties": { ... }\n}`,
                    description: "Generated automatically from the sample parameters object when left empty.",
                }}
            />
            <FormInput
                control={control}
                name={`review.actions.${index}.url`}
                label="Webhook URL"
                placeholder="https://example.com/hooks/create-ticket"
                description="Quill POSTs the parameters the LLM supplied to this URL and feeds the response back to the agent."
            />
            <FormInput
                control={control}
                name={`review.actions.${index}.secret`}
                label="Secret"
                placeholder="Optional"
                description="Sent as the X-Quill-Secret header so the receiver can verify the call came from Quill."
            />
            <FormInput
                control={control}
                name={`review.actions.${index}.maxResponseSize`}
                type="number"
                label="Max response size (optional)"
                placeholder={`${DEFAULT_ACTION_RESPONSE_BYTES} (default)`}
                description="Bytes of the response passed back to the agent. The rest is dropped and marked truncated, and every byte kept costs tokens."
            />
        </ConfigItemCard>
    );
}

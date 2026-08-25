import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { Text } from "@/components/typography";
import { Plus, Settings2 } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormSwitch } from "@/components/form/form-switch";
import { FormTextarea } from "@/components/form/form-textarea";
import type {
    AgentFormData,
    AgentParameterFormData,
} from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { emptyAgentParameter } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { ConfigItemCard, ConfigListEmpty } from "@/pages/setup/add-capability-wizard/steps/review/config-item-card";
import { cn } from "@/lib/utils";

const PARAMETER_TYPE_OPTIONS: FormSelectOption<AgentParameterFormData["type"]>[] = [
    { value: "String", label: "String" },
    { value: "Number", label: "Number" },
    { value: "Boolean", label: "Boolean" },
    { value: "ArrayOfString", label: "String[]" },
    { value: "ArrayOfNumber", label: "Number[]" },
    { value: "ArrayOfBoolean", label: "Boolean[]" },
    { value: "Default", label: "Any" },
    { value: "Null", label: "Null" },
];

const PARAMETER_POLICY_OPTIONS: FormSelectOption<AgentParameterFormData["policy"]>[] = [
    { value: "Default", label: "Default" },
    { value: "ForbidModelGeneration", label: "Forbid model generation" },
];

export function AgentParametersSection({ className }: { className?: string }) {
    const { control } = useFormContext<AgentFormData>();
    const fieldArray = useFieldArray({ control, name: "review.parameters" });

    return (
        <div className={cn("grid gap-3 rounded-lg border bg-background p-4", className)}>
            <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                    <Settings2 className="size-4 text-muted-foreground" />
                    <Text variant="label" as="span">
                        Agent parameters
                    </Text>
                </div>
                <Button variant="outline" size="sm" onClick={() => fieldArray.append(emptyAgentParameter())}>
                    <Plus />
                    Add new parameter
                </Button>
            </div>

            {fieldArray.fields.length === 0 ? (
                <ConfigListEmpty label="No parameters have been defined yet" />
            ) : (
                <div className="grid gap-2">
                    {fieldArray.fields.map((field, index) => (
                        <ParameterItem key={field.id} index={index} remove={() => fieldArray.remove(index)} />
                    ))}
                </div>
            )}
        </div>
    );
}

function ParameterItem({ index, remove }: { index: number; remove: () => void }) {
    const { control, setValue } = useFormContext<AgentFormData>();
    const parameter = useWatch({ control, name: `review.parameters.${index}` });

    if (!parameter) {
        return null;
    }

    return (
        <ConfigItemCard
            isExpanded={parameter.isExpanded}
            editTitle="Configure parameter"
            summary={
                <>
                    <div className="flex min-w-0 items-center gap-2">
                        <Text variant="label" as="span" className="truncate">
                            {parameter.name || "(unnamed)"}
                        </Text>
                        <Text variant="caption" as="span">
                            |
                        </Text>
                        <Text variant="caption" as="span" className="shrink-0 font-mono">
                            {getParameterTypeLabel(parameter.type)}
                        </Text>
                    </div>
                    {parameter.description && (
                        <Text variant="caption" className="mt-0.5 truncate">
                            {parameter.description}
                        </Text>
                    )}
                </>
            }
            onToggleExpanded={(isExpanded) => setValue(`review.parameters.${index}.isExpanded`, isExpanded)}
            onRemove={remove}
        >
            <div className="grid gap-4 sm:grid-cols-2">
                <FormInput
                    control={control}
                    name={`review.parameters.${index}.name`}
                    label="Parameter name"
                    placeholder="e.g. company"
                />
                <FormSelect
                    control={control}
                    name={`review.parameters.${index}.type`}
                    label="Parameter type"
                    options={PARAMETER_TYPE_OPTIONS}
                />
            </div>
            <FormTextarea
                control={control}
                name={`review.parameters.${index}.description`}
                label="Description (optional)"
                placeholder="e.g. The company ID"
                rows={2}
            />
            <FormSelect
                control={control}
                name={`review.parameters.${index}.policy`}
                label="Forbid model generation"
                options={PARAMETER_POLICY_OPTIONS}
                description="When forbidden and this agent is used as a sub-agent, the parent agent cannot generate this parameter's value; it may only be inherited from the parent agent parameters."
            />
            <FormSwitch control={control} name={`review.parameters.${index}.isSendToModel`} label="Send to model" />
        </ConfigItemCard>
    );
}

function getParameterTypeLabel(type: AgentParameterFormData["type"]) {
    return PARAMETER_TYPE_OPTIONS.find((option) => option.value === type)?.label ?? type;
}

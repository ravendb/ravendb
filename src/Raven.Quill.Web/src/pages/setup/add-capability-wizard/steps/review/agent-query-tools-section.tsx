import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { Text } from "@/components/typography";
import { Plus, SearchCode } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { FormAceEditor } from "@/components/form/form-ace-editor";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormTextarea } from "@/components/form/form-textarea";
import type {
    AgentFormData,
    AgentQueryToolFormData,
} from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { emptyAgentQueryTool } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { ConfigItemCard, ConfigListEmpty } from "@/pages/setup/add-capability-wizard/steps/review/config-item-card";
import { SampleObjectAndSchemaTabs } from "@/pages/setup/add-capability-wizard/steps/review/sample-object-and-schema-tabs";
import AceEditor from "@/components/ace-editor/ace-editor";
import { cn } from "@/lib/utils";

const TRI_STATE_OPTIONS: FormSelectOption<AgentQueryToolFormData["allowModelQueries"]>[] = [
    { value: "Default", label: "Default" },
    { value: "True", label: "True" },
    { value: "False", label: "False" },
];

const QUERY_PLACEHOLDER = `from "Orders" where ShipTo.Country == $country`;

const DESCRIPTION_PLACEHOLDER =
    "Explain to the LLM when it should trigger this query. " +
    "E.g.: Use this query to retrieve Order documents filtered by destination country.";

export function AgentQueryToolsSection({ className }: { className?: string }) {
    const { control } = useFormContext<AgentFormData>();
    const fieldArray = useFieldArray({ control, name: "review.queries" });

    return (
        <div className={cn("grid gap-3 rounded-lg border bg-background p-4", className)}>
            <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                    <SearchCode className="size-4 text-muted-foreground" />
                    <Text variant="label" as="span">
                        Query tools
                    </Text>
                </div>
                <Button variant="outline" size="sm" onClick={() => fieldArray.append(emptyAgentQueryTool())}>
                    <Plus />
                    Add new query tool
                </Button>
            </div>

            {fieldArray.fields.length === 0 ? (
                <ConfigListEmpty label="No query tools have been defined yet" />
            ) : (
                <div className="grid gap-2">
                    {fieldArray.fields.map((field, index) => (
                        <QueryToolItem key={field.id} index={index} remove={() => fieldArray.remove(index)} />
                    ))}
                </div>
            )}
        </div>
    );
}

function QueryToolItem({ index, remove }: { index: number; remove: () => void }) {
    const { control, setValue } = useFormContext<AgentFormData>();
    const tool = useWatch({ control, name: `review.queries.${index}` });

    if (!tool) {
        return null;
    }

    return (
        <ConfigItemCard
            isExpanded={tool.isExpanded}
            editTitle="Configure query tool"
            summary={
                <>
                    <Text variant="label" className="truncate">
                        {tool.name || "(unnamed)"}
                    </Text>
                    {tool.description && (
                        <Text variant="caption" className="mt-0.5 truncate">
                            {tool.description}
                        </Text>
                    )}
                </>
            }
            onToggleExpanded={(isExpanded) => setValue(`review.queries.${index}.isExpanded`, isExpanded)}
            onRemove={remove}
        >
            <FormInput
                control={control}
                name={`review.queries.${index}.name`}
                label="Tool name"
                placeholder="e.g. GetOrdersByCountry"
            />
            <FormTextarea
                control={control}
                name={`review.queries.${index}.description`}
                label="Description"
                placeholder={DESCRIPTION_PLACEHOLDER}
                rows={3}
            />
            <FormAceEditor
                control={control}
                name={`review.queries.${index}.query`}
                label="Query"
                mode="sql"
                height="120px"
                description="The RQL query the agent runs against the database when the LLM triggers this tool."
                placeholder={QUERY_PLACEHOLDER}
                actions={[
                    { component: <AceEditor.FullScreenAction /> },
                    { component: <AceEditor.FormatAction /> },
                    { component: <AceEditor.AutoResizeHeightAction /> },
                ]}
            />
            <SampleObjectAndSchemaTabs
                sampleObject={{
                    name: `review.queries.${index}.parametersSampleObject`,
                    label: "Sample parameters object",
                    placeholder: `{\n    // "ParamName": "Instruction to the LLM"\n}`,
                    description:
                        "A JSON object defining the parameters the LLM should supply when it requests this query.",
                }}
                schema={{
                    name: `review.queries.${index}.parametersSchema`,
                    label: "Parameters JSON schema",
                    placeholder: `{\n    "type": "object",\n    "properties": { ... }\n}`,
                    description: "Generated automatically from the sample parameters object when left empty.",
                }}
            />
            <div className="grid gap-4 sm:grid-cols-2">
                <FormSelect
                    control={control}
                    name={`review.queries.${index}.allowModelQueries`}
                    label="Allow model queries"
                    options={TRI_STATE_OPTIONS}
                    description="Whether the model may execute this query on demand."
                />
                <FormSelect
                    control={control}
                    name={`review.queries.${index}.addToInitialContext`}
                    label="Add to initial context"
                    options={TRI_STATE_OPTIONS}
                    description="Whether to run this query upfront and hand its results to the model."
                />
            </div>
        </ConfigItemCard>
    );
}

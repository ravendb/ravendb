import { useState, type ReactNode } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { PanelRightClose, PanelRightOpen, Sparkles } from "lucide-react";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/shadcn/ui/badge";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { SuggestionPicker } from "@/pages/setup/add-capability-wizard/suggestion-picker";

type AgentSuggestionTabProps = {
    // Jumps to the "Agent configuration" tab, where the prompt is edited.
    showConfiguration: () => void;
};

// Read-only overview of the AI-suggested agent: pick a candidate, see its summary and
// the full system prompt. All values come from the editable configuration, so edits
// made in the "Agent configuration" tab are reflected here.
export function AgentSuggestionTab({ showConfiguration }: AgentSuggestionTabProps) {
    const { control } = useFormContext<AgentFormData>();
    const config = useWatch({ control, name: "review" });
    const connectionStringName = useWatch({ control, name: "connection.connectionStringName" });
    const [isPromptHidden, setIsPromptHidden] = useState(false);

    const systemPrompt = config.systemPrompt?.trim() ?? "";
    const isPromptPanelVisible = Boolean(systemPrompt) && !isPromptHidden;
    const parameterNames = (config.parameters ?? []).map((parameter) => parameter.name).filter(Boolean);
    const queryToolNames = (config.queries ?? []).map((query) => query.name).filter(Boolean);

    return (
        <div className="grid gap-5">
            <SuggestionPicker />

            <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 text-sm font-medium">
                    <Sparkles className="size-4" />
                    AI Suggest
                </div>
                {systemPrompt && (
                    <button
                        type="button"
                        onClick={() => setIsPromptHidden((isHidden) => !isHidden)}
                        className="flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
                    >
                        {isPromptHidden ? "Show prompt" : "Hide prompt"}
                        {isPromptHidden ? (
                            <PanelRightOpen className="size-4" />
                        ) : (
                            <PanelRightClose className="size-4" />
                        )}
                    </button>
                )}
            </div>

            <div className={cn("grid items-start gap-4", isPromptPanelVisible && "lg:grid-cols-[2fr_minmax(0,1fr)]")}>
                <div className="grid min-w-0 gap-2">
                    <span className="text-sm font-medium">Agent summary</span>
                    <div className="flex flex-col divide-y rounded-lg border bg-background px-4">
                        <SummaryRow label="Agent name">{config.name || "—"}</SummaryRow>
                        <SummaryRow label="System prompt">
                            {systemPrompt ? (
                                <TooltipProvider>
                                    <Tooltip>
                                        <TooltipTrigger asChild>
                                            <span className="block max-w-md cursor-default">{systemPrompt}</span>
                                        </TooltipTrigger>
                                        <TooltipContent className="max-w-sm whitespace-normal">
                                            {systemPrompt}
                                        </TooltipContent>
                                    </Tooltip>
                                </TooltipProvider>
                            ) : (
                                "—"
                            )}
                        </SummaryRow>
                        <SummaryRow label="Connection string">{connectionStringName || "—"}</SummaryRow>
                        <SummaryRow label="Parameters">
                            <ChipList items={parameterNames} />
                        </SummaryRow>
                        <SummaryRow label="Query tools">
                            <ChipList items={queryToolNames} />
                        </SummaryRow>
                        <SummaryRow label="Action tools">—</SummaryRow>
                    </div>
                </div>

                {isPromptPanelVisible && (
                    <div className="grid min-w-0 gap-2">
                        <div className="flex items-center justify-between gap-3">
                            <span className="text-sm font-medium">Your prompt (TODO)</span>
                            <button
                                type="button"
                                onClick={showConfiguration}
                                className="text-xs text-muted-foreground underline-offset-2 hover:text-foreground hover:underline"
                            >
                                Edit your prompt (TODO)
                            </button>
                        </div>
                        <div className="rounded-lg border bg-background p-3 text-sm whitespace-pre-wrap">
                            {systemPrompt}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

function SummaryRow({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div className="flex min-h-12 items-center justify-between gap-4 py-3">
            <span className="shrink-0 text-sm font-medium">{label}</span>
            <div className="flex min-w-0 justify-end text-sm">{children}</div>
        </div>
    );
}

function ChipList({ items }: { items: string[] }) {
    if (items.length === 0) {
        return <span className="text-muted-foreground">—</span>;
    }

    return (
        <div className="flex flex-wrap justify-end gap-1.5">
            {items.map((item) => (
                <Badge key={item} variant="secondary" className="font-mono">
                    {item}
                </Badge>
            ))}
        </div>
    );
}

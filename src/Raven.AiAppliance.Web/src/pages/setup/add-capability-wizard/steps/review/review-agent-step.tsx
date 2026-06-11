import { useState } from "react";
import { useFormContext, useFormState, useWatch } from "react-hook-form";
import { CircleAlert } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AgentConfigurationTab } from "@/pages/setup/add-capability-wizard/steps/review/agent-configuration-tab";
import { AgentSuggestionTab } from "@/pages/setup/add-capability-wizard/steps/review/agent-suggestion-tab";

type ReviewTabId = "suggestion" | "configuration";

export function ReviewAgentStep() {
    const { control } = useFormContext<AgentFormData>();
    const mode = useWatch({ control, name: "create.mode" });
    const { errors } = useFormState({ control, name: "review" });
    const [activeTab, setActiveTab] = useState<ReviewTabId>("suggestion");

    // Manual setup skips the AI suggestion overview — there is no suggestion to show.
    if (mode === "manual") {
        return <AgentConfigurationTab />;
    }

    return (
        <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as ReviewTabId)}>
            <TabsList>
                <TabsTrigger value="suggestion">AI suggestion</TabsTrigger>
                <TabsTrigger value="configuration">
                    Agent configuration
                    {/* The wizard's Next validates the configuration even when this tab is
                        not active; the icon points at where the errors live. */}
                    {errors.review && <CircleAlert className="size-3.5 text-destructive" />}
                </TabsTrigger>
            </TabsList>
            <TabsContent value="suggestion" className="mt-3">
                <AgentSuggestionTab showConfiguration={() => setActiveTab("configuration")} />
            </TabsContent>
            <TabsContent value="configuration" className="mt-3">
                <AgentConfigurationTab />
            </TabsContent>
        </Tabs>
    );
}

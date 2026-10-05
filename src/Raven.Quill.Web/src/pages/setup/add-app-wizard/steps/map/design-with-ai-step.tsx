import { useFormContext } from "react-hook-form";
import { Sparkles, Table2 } from "lucide-react";
import { AiConsentAcceptedNote } from "@/components/ai-consent/ai-consent-accepted-note";
import { AiConsentGate, type AiConsentCopy } from "@/components/ai-consent/ai-consent-gate";
import { TranscriptDisclosureState } from "@/components/chat/transcript-disclosure";
import { Button } from "@/components/shadcn/ui/button";
import {
    ResizableHandle,
    ResizablePanel,
    ResizablePanelGroup,
} from "@/components/shadcn/ui/resizable";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { PlannerChat } from "@/pages/setup/add-app-wizard/steps/map/planner-chat";
import { PlannerResults } from "@/pages/setup/add-app-wizard/steps/map/planner-results";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/use-planner-session";
import { useAdoptMapTables } from "@/pages/setup/add-app-wizard/steps/map/use-adopt-map-tables";
import { scaffoldTables } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

const PLANNER_CONSENT_COPY: AiConsentCopy = {
    gateDescription:
        "Designing the mapping with AI sends the selected tables' schema to the RavenDB AI service. Accept its " +
        "Terms of Use to start a session - or use manual mapping instead.",
    dialogTitle: "Design your mapping with AI",
    dialogDescription:
        "AI reads the schema of the tables you selected - names, columns, types and keys, never row data - and " +
        "proposes how they become collections. That sends the schema to the RavenDB AI service, so it is " +
        "available only once you accept its Terms of Use.",
};

export function DesignWithAiStep({ isBusy }: WizardBodyComponentProps) {
    const { getValues } = useFormContext<AppFormData>();
    const session = usePlannerSession();
    const adoptTables = useAdoptMapTables();

    const hasConversation = session.conversationId !== null;

    const useManualMapping = () => {
        const store = useSetupWizardStore.getState();
        adoptTables(scaffoldTables(getValues("verifySchema").tables, store.discoverResult));
    };

    return (
        <TranscriptDisclosureState>
            <div className="flex min-h-0 flex-1 flex-col gap-3">
                <AiConsentGate variant="banner" copy={PLANNER_CONSENT_COPY} />
                <div className="flex shrink-0 items-center justify-end gap-2">
                    <div className="mr-auto min-w-0">
                        <AiConsentAcceptedNote />
                    </div>
                    <Button
                        type="button"
                        onClick={() => void session.start()}
                        disabled={isBusy || session.isStreaming}
                    >
                        <Sparkles aria-hidden />
                        {hasConversation ? "Start over" : "Start"}
                    </Button>
                    <Button
                        type="button"
                        variant="outline"
                        onClick={useManualMapping}
                        disabled={isBusy || session.isStreaming}
                    >
                        <Table2 aria-hidden />
                        Use manual mapping
                    </Button>
                </div>

                <ResizablePanelGroup
                    orientation="horizontal"
                    className="min-h-80 flex-1 rounded-lg border bg-background"
                >
                    <ResizablePanel defaultSize="40%" minSize="320px" maxSize="60%" className="min-w-0">
                        <PlannerChat
                            isStreaming={session.isStreaming}
                            canAsk={hasConversation}
                            onAsk={(prompt) => void session.ask(prompt)}
                            onStop={session.stop}
                        />
                    </ResizablePanel>
                    <ResizableHandle />
                    <ResizablePanel className="min-w-0">
                        <PlannerResults />
                    </ResizablePanel>
                </ResizablePanelGroup>
            </div>
        </TranscriptDisclosureState>
    );
}

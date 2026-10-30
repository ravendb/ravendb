import { MessageSquare, RotateCcw } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { Spinner } from "@/components/shadcn/ui/spinner";
import type { WizardHeaderComponentProps } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { selectHasSession, selectIsAnyChatWorking } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";

export function PlannerHeaderActions({ isBusy }: WizardHeaderComponentProps) {
    const hasSession = useSetupWizardStore(selectHasSession);
    const isWorking = useSetupWizardStore(selectIsAnyChatWorking);
    const isChatOpen = useSetupWizardStore((state) => state.plannerIsChatOpen);
    const setPlannerChatOpen = useSetupWizardStore((state) => state.setPlannerChatOpen);
    const startPlannerOver = useSetupWizardStore((state) => state.startPlannerOver);

    if (!hasSession) {
        return null;
    }

    return (
        <div className="flex items-center gap-2">
            {!isChatOpen && (
                <Button variant="outline" size="sm" onClick={() => setPlannerChatOpen(true)}>
                    {isWorking ? (
                        <Spinner className="text-primary-strong" data-icon="inline-start" />
                    ) : (
                        <MessageSquare data-icon="inline-start" />
                    )}
                    Chat
                </Button>
            )}
            <ConfirmDialog
                trigger={
                    <Button variant="outline" size="sm" disabled={isWorking || isBusy}>
                        <RotateCcw data-icon="inline-start" />
                        Start over
                    </Button>
                }
                variant="destructive"
                icon={null}
                title="Start over?"
                description="This clears every conversation, the plan and your edits, including the built collections. The tables you selected stay as they are."
                confirmLabel="Start over"
                onConfirm={startPlannerOver}
            />
        </div>
    );
}

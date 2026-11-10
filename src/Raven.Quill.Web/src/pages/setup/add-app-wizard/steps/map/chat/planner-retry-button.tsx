import { RotateCcw } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { useCanSendMessage } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanArea } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { isBuildTurn, selectChat } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { PlannerChatId } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

/** Runs the chat's failed turn again. A failed build that left nothing to build has nothing to run again. */
export function PlannerRetryButton({ chatId, area }: { chatId: PlannerChatId; area: PlanArea | null }) {
    const session = usePlannerSession();
    const canSendMessage = useCanSendMessage();
    const isBuild = useSetupWizardStore((state) => area !== null && isBuildTurn(selectChat(state, chatId)));
    const pendingCollections = area?.pendingCollections ?? [];

    if (isBuild && pendingCollections.length === 0) {
        return null;
    }

    return (
        <Button
            variant="outline"
            size="sm"
            className="self-start"
            disabled={!canSendMessage}
            onClick={() => void session.retry(chatId, pendingCollections)}
        >
            <RotateCcw data-icon="inline-start" />
            Retry
        </Button>
    );
}

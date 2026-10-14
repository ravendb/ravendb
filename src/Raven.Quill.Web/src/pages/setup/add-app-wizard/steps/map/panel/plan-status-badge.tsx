import { StatusIndicator, type StatusTone } from "@/components/data/status-indicator";
import {
    PLAN_STATE_LABELS,
    type PlanCollectionState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

const STATE_TONES: Record<PlanCollectionState, StatusTone> = {
    suggested: "muted",
    built: "positive",
    updated: "info",
    failed: "danger",
};

export function PlanStatusBadge({ state, className }: { state: PlanCollectionState; className?: string }) {
    return <StatusIndicator tone={STATE_TONES[state]} label={PLAN_STATE_LABELS[state]} className={className} />;
}

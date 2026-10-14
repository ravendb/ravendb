import type { ReactNode } from "react";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import {
    ActivityRowLayout,
    DoneIcon,
    PendingIcon,
    PlannerWorkingCard,
    WorkingIcon,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity-parts";
import { PlannerMessage } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-message";
import { PlannerSelectedTables } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-selected-tables";
import { usePlannerWorkspace } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlannerReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

type StepState = "done" | "active" | "pending";

const STEP_COUNT = 3;

/**
 * The planner's first turn while it runs. The schema goes out with the request, so reading it is done
 * from the start; the proposal ends the grouping, and the reply ends the questions.
 */
export function PlannerOpeningProgress({ turn }: { turn: PlannerReplyTurn }) {
    const { changeSelection } = usePlannerWorkspace();
    const tables = useSetupWizardStore((state) => state.plannerSelectedTables) ?? [];
    const hasProposal = turn.activity.some((activity) => activity.type === "proposal");
    const isSingleTable = tables.length === 1;

    return (
        <PlannerMessage>
            <Text>
                I&apos;m reading the schema of the <strong>{formatCount(tables.length, "table")}</strong> you selected
                (names, columns, types and keys, never row data) to suggest how{" "}
                {isSingleTable ? "it becomes" : "they become"} collections. Nothing reaches the mapping until you apply
                it.
            </Text>
            <PlannerWorkingCard title="Planning collections" detail={`Step ${hasProposal ? 3 : 2} of ${STEP_COUNT}`}>
                <li className="flex flex-col gap-1.5">
                    <PlannerSelectedTables tables={tables} onChangeSelection={changeSelection} />
                </li>
                <ProgressStep state={hasProposal ? "done" : "active"}>
                    {hasProposal ? "Grouped tables into collections" : "Grouping tables into collections…"}
                </ProgressStep>
                <ProgressStep state={hasProposal ? "active" : "pending"}>
                    {hasProposal
                        ? "Asking about anything the schema can't answer…"
                        : "Ask about anything the schema can't answer"}
                </ProgressStep>
            </PlannerWorkingCard>
        </PlannerMessage>
    );
}

const STEP_ICONS: Record<StepState, ReactNode> = {
    done: <DoneIcon />,
    active: <WorkingIcon />,
    pending: <PendingIcon />,
};

function ProgressStep({ state, children }: { state: StepState; children: ReactNode }) {
    return (
        <ActivityRowLayout icon={STEP_ICONS[state]} className={state === "pending" ? "opacity-60" : undefined}>
            {children}
        </ActivityRowLayout>
    );
}

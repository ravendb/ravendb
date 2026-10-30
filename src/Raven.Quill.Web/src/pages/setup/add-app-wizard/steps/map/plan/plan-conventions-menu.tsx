import { ChevronDown, Lock } from "lucide-react";
import type { PropertyCase } from "@/api/generated/server-api";
import { Button } from "@/components/shadcn/ui/button";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuLabel,
    DropdownMenuRadioGroup,
    DropdownMenuRadioItem,
    DropdownMenuTrigger,
} from "@/components/shadcn/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Text } from "@/components/typography";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { useCanAsk } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import { composeConventionsPrompt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import {
    selectAreConventionsLocked,
    selectHasUnsavedEdits,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

type CaseOption = { value: Exclude<PropertyCase, "Unspecified">; label: string; example: string };

const CASE_OPTIONS: CaseOption[] = [
    { value: "CamelCase", label: "camelCase", example: "orderDate" },
    { value: "PascalCase", label: "PascalCase", example: "OrderDate" },
    { value: "SnakeCase", label: "snake_case", example: "order_date" },
];

const LOCKED_NOTE =
    "Locked since the first area started building. To use other names, edit the mappings by hand in their areas.";

const OPEN_NOTE = "You can change it until the first area starts building. After that it is locked for every area.";

const BUSY_NOTE = "The planner is busy. You can change it once it is done.";

const UNSAVED_NOTE = "Save or discard your changes first.";

/** Every area builds with the conventions in force when it started, so they lock once the first area starts. */
export function PlanConventionsMenu() {
    const session = usePlannerSession();
    const canAsk = useCanAsk(null);
    const conventions = useSetupWizardStore((state) => state.plannerConventions);
    const isLocked = useSetupWizardStore(selectAreConventionsLocked);
    const hasUnsavedEdits = useSetupWizardStore(selectHasUnsavedEdits);
    const setPlannerChatOpen = useSetupWizardStore((state) => state.setPlannerChatOpen);

    const isDisabled = isLocked || !canAsk;
    const disabledNote = isLocked ? LOCKED_NOTE : hasUnsavedEdits ? UNSAVED_NOTE : BUSY_NOTE;
    const current = CASE_OPTIONS.find((option) => option.value === conventions?.propertyCase);
    const label = [current?.label ?? "Default", conventions?.propertyLanguage].filter(Boolean).join(", ");
    const trigger = (
        <Button
            variant="outline"
            size="sm"
            className="h-8"
            aria-disabled={isDisabled ? true : undefined}
            onClick={(event) => isDisabled && event.preventDefault()}
        >
            <Text as="span" variant="muted">
                Property names
            </Text>
            <span className={isLocked ? "text-muted-foreground" : undefined}>{label}</span>
            {isLocked ? <Lock data-icon="inline-end" /> : <ChevronDown data-icon="inline-end" />}
        </Button>
    );

    if (isDisabled) {
        return (
            <Tooltip>
                <TooltipTrigger asChild>{trigger}</TooltipTrigger>
                <TooltipContent className="max-w-72">{disabledNote}</TooltipContent>
            </Tooltip>
        );
    }

    return (
        <DropdownMenu>
            <DropdownMenuTrigger asChild>{trigger}</DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-72">
                <DropdownMenuLabel className="font-normal text-muted-foreground">
                    A column named order_date becomes
                </DropdownMenuLabel>
                <DropdownMenuRadioGroup
                    value={current?.value ?? ""}
                    onValueChange={(value) => {
                        const option = CASE_OPTIONS.find((item) => item.value === value);

                        if (option) {
                            setPlannerChatOpen(true);
                            void session.askProposal({ text: composeConventionsPrompt(option.label), mentions: [] });
                        }
                    }}
                >
                    {CASE_OPTIONS.map((option) => (
                        <DropdownMenuRadioItem key={option.value} value={option.value}>
                            {option.label}
                            <Text as="span" variant="code" className="ml-auto text-muted-foreground">
                                {option.example}
                            </Text>
                        </DropdownMenuRadioItem>
                    ))}
                </DropdownMenuRadioGroup>
                <Text variant="caption" className="border-t px-2 pt-2 pb-1">
                    {current ? `Chosen with your answers. ${OPEN_NOTE}` : OPEN_NOTE}
                </Text>
            </DropdownMenuContent>
        </DropdownMenu>
    );
}

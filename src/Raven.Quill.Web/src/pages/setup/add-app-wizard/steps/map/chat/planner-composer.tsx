import { useRef, useState, type ChangeEvent } from "react";
import { ArrowUp, AtSign, Square } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import {
    Command,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
} from "@/components/shadcn/ui/command";
import { InputGroup, InputGroupAddon, InputGroupTextarea } from "@/components/shadcn/ui/input-group";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/shadcn/ui/popover";
import { Text } from "@/components/typography";
import { isSendMessageKey } from "@/lib/form-utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { describeWork } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { PlannerMentionChip } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-mention-chip";
import {
    useCanAskPlanner,
    useCanComposeForPlanner,
    usePlannerWorkspace,
} from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { describeMentionPlaceholder } from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import { getLatestReply, selectPlannerPhase } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import {
    hasUnbuiltMention,
    usePlannerSession,
} from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

const MENTION_TRIGGER = "@";

/**
 * Where the operator talks to the planner. While the first plan is being made it collects instructions
 * that go in once that plan is ready; after that it sends prompts, with collections mentioned by `@`.
 */
export function PlannerComposer({ collections }: { collections: PlanCollection[] }) {
    const { composerRef } = usePlannerWorkspace();
    const session = usePlannerSession();
    const canAsk = useCanAskPlanner();
    const canCompose = useCanComposeForPlanner();
    const phase = useSetupWizardStore(selectPlannerPhase);
    const latestReply = useSetupWizardStore((state) => getLatestReply(state.plannerTurns));
    const draft = useSetupWizardStore((state) => state.plannerDraft);
    const mentions = useSetupWizardStore((state) => state.plannerMentions);
    const canSendPicksAlone = useSetupWizardStore(hasUnbuiltMention);
    const setPlannerDraft = useSetupWizardStore((state) => state.setPlannerDraft);
    const addPlannerMention = useSetupWizardStore((state) => state.addPlannerMention);
    const removePlannerMention = useSetupWizardStore((state) => state.removePlannerMention);
    const [isMentionPickerOpen, setIsMentionPickerOpen] = useState(false);
    // Where the typed MENTION_TRIGGER sits, so picking a collection takes it out of the text, and where the caret goes back to.
    const mentionTriggerIndexRef = useRef<number | null>(null);
    const caretAfterPickerRef = useRef<number | null>(null);

    const isOpening = phase === "opening";
    const isWorking = isOpening || phase === "working";
    const canSend = canAsk && (draft.trim() !== "" || canSendPicksAlone);

    // Typing MENTION_TRIGGER where a word starts opens the picker, which adds the collection as a chip. Pasted text,
    // deleting, or backing out of the picker leave the MENTION_TRIGGER as written.
    const changeDraft = (event: ChangeEvent<HTMLTextAreaElement>) => {
        const { value, selectionStart } = event.target;
        const input = event.nativeEvent as Partial<InputEvent>;
        const isTriggerTyped = input.inputType === "insertText" && input.data === MENTION_TRIGGER;
        const isWordStart = selectionStart === 1 || /\s/.test(value.charAt(selectionStart - 2));

        setPlannerDraft(value);

        if (canAsk && isTriggerTyped && isWordStart) {
            mentionTriggerIndexRef.current = selectionStart - 1;
            setIsMentionPickerOpen(true);
        }
    };

    const changeMentionPickerOpen = (isOpen: boolean) => {
        if (isOpen) {
            mentionTriggerIndexRef.current = null;
        }

        setIsMentionPickerOpen(isOpen);
    };

    const pickMention = (collection: string) => {
        const triggerIndex = mentionTriggerIndexRef.current;
        mentionTriggerIndexRef.current = null;

        if (triggerIndex !== null && draft.charAt(triggerIndex) === MENTION_TRIGGER) {
            setPlannerDraft(draft.slice(0, triggerIndex) + draft.slice(triggerIndex + 1));
            caretAfterPickerRef.current = triggerIndex;
        }

        addPlannerMention(collection);
    };

    const escapeMentionPicker = () => {
        const triggerIndex = mentionTriggerIndexRef.current;
        mentionTriggerIndexRef.current = null;
        caretAfterPickerRef.current = triggerIndex === null ? null : triggerIndex + 1;
    };

    const returnToComposer = () => {
        const composer = composerRef.current;
        const caret = caretAfterPickerRef.current;
        caretAfterPickerRef.current = null;

        composer?.focus();

        if (composer && caret !== null) {
            composer.setSelectionRange(caret, caret);
        }
    };

    return (
        <InputGroup className="rounded-xl bg-card dark:bg-card">
            {mentions.length > 0 && (
                <InputGroupAddon align="block-start" className="flex-wrap gap-1.5">
                    {mentions.map((collection) => (
                        <PlannerMentionChip
                            key={collection}
                            collection={collection}
                            onRemove={() => removePlannerMention(collection)}
                        />
                    ))}
                </InputGroupAddon>
            )}
            <InputGroupTextarea
                ref={composerRef}
                rows={1}
                aria-label={isOpening ? "Instructions for the planner" : "Prompt for the planner"}
                placeholder={describePlaceholder({ isOpening, isWorking, isFailed: phase === "failed", mentions })}
                value={draft}
                disabled={!canCompose}
                className="max-h-48 min-h-10 px-3.5"
                onChange={changeDraft}
                onKeyDown={(event) => {
                    if (isSendMessageKey(event)) {
                        event.preventDefault();

                        if (canSend) {
                            void session.sendDraft();
                        }
                    }
                }}
            />
            <InputGroupAddon align="block-end" className="gap-2 pt-0">
                {isWorking ? (
                    <>
                        <Text variant="caption" as="span">
                            {isOpening
                                ? "Optional. Sent once the first plan is ready, and the planner revises it."
                                : latestReply && describeWork(latestReply)}
                        </Text>
                        <Button
                            variant="secondary"
                            size="icon-sm"
                            className="ml-auto"
                            aria-label="Stop"
                            onClick={session.stop}
                        >
                            <Square className="fill-current" aria-hidden="true" />
                        </Button>
                    </>
                ) : (
                    canAsk && (
                        <>
                            <MentionPicker
                                collections={collections.filter((collection) => !mentions.includes(collection.name))}
                                isOpen={isMentionPickerOpen}
                                onOpenChange={changeMentionPickerOpen}
                                onPick={pickMention}
                                onEscape={escapeMentionPicker}
                                onReturnToComposer={returnToComposer}
                            />
                            <Button
                                size="icon-sm"
                                className="ml-auto"
                                aria-label="Send"
                                disabled={!canSend}
                                onClick={() => void session.sendDraft()}
                            >
                                <ArrowUp aria-hidden="true" />
                            </Button>
                        </>
                    )
                )}
            </InputGroupAddon>
        </InputGroup>
    );
}

function MentionPicker({
    collections,
    isOpen,
    onOpenChange,
    onPick,
    onEscape,
    onReturnToComposer,
}: {
    collections: PlanCollection[];
    isOpen: boolean;
    onOpenChange: (isOpen: boolean) => void;
    onPick: (collection: string) => void;
    onEscape: () => void;
    onReturnToComposer: () => void;
}) {
    // Picking a collection, or backing out with Escape, carries on writing; a click elsewhere goes where it went.
    const shouldReturnToComposerRef = useRef(false);

    return (
        <Popover open={isOpen} onOpenChange={onOpenChange}>
            <PopoverTrigger asChild>
                <Button
                    variant="ghost"
                    size="icon-sm"
                    className="text-muted-foreground"
                    aria-label="Mention a collection"
                >
                    <AtSign aria-hidden="true" />
                </Button>
            </PopoverTrigger>
            <PopoverContent
                align="start"
                side="top"
                className="w-64 p-0"
                onEscapeKeyDown={() => {
                    shouldReturnToComposerRef.current = true;
                    onEscape();
                }}
                onCloseAutoFocus={(event) => {
                    if (shouldReturnToComposerRef.current) {
                        event.preventDefault();
                        onReturnToComposer();
                    }

                    shouldReturnToComposerRef.current = false;
                }}
            >
                <Command>
                    <CommandInput placeholder="Find a collection" />
                    <CommandList>
                        <CommandEmpty>No collection matches.</CommandEmpty>
                        <CommandGroup>
                            {collections.map((collection) => (
                                <CommandItem
                                    key={collection.name}
                                    value={collection.name}
                                    onSelect={() => {
                                        shouldReturnToComposerRef.current = true;
                                        onPick(collection.name);
                                        onOpenChange(false);
                                    }}
                                >
                                    {collection.name}
                                </CommandItem>
                            ))}
                        </CommandGroup>
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
    );
}

function describePlaceholder({
    isOpening,
    isWorking,
    isFailed,
    mentions,
}: {
    isOpening: boolean;
    isWorking: boolean;
    isFailed: boolean;
    mentions: string[];
}): string {
    if (isOpening) {
        return "Anything the planner should know? For example: orders are always read with their lines.";
    }

    if (isWorking) {
        return "The planner is working. Write your next prompt, and send it once it is done.";
    }

    if (isFailed) {
        return "Start over to run the planner again.";
    }

    return mentions.length > 0
        ? describeMentionPlaceholder(mentions)
        : `Ask the planner for a change. Type ${MENTION_TRIGGER} to mention a collection.`;
}

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
import { isSendMessageKey } from "@/lib/form-utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlannerMentionChip } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-mention-chip";
import {
    useCanSendMessage,
    useChatPhase,
    usePlannerWorkspace,
} from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { describeMentionPlaceholder } from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import { selectChat } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { PlannerChatId } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

const MENTION_TRIGGER = "@";

/** While the agent works, what the operator writes waits here, to be sent once it is done or stopped. */
export function PlannerComposer({ chatId, collections }: { chatId: PlannerChatId; collections: PlanCollection[] }) {
    const { composerRef, isBusy } = usePlannerWorkspace();
    const session = usePlannerSession();
    const canSendMessage = useCanSendMessage(chatId);
    const phase = useChatPhase(chatId);
    const hasProposal = useSetupWizardStore((state) => state.plannerProposal !== null);
    const draft = useSetupWizardStore((state) => selectChat(state, chatId).draft);
    const mentions = useSetupWizardStore((state) => selectChat(state, chatId).mentions);
    const setPlannerDraft = useSetupWizardStore((state) => state.setPlannerDraft);
    const addPlannerMention = useSetupWizardStore((state) => state.addPlannerMention);
    const removePlannerMention = useSetupWizardStore((state) => state.removePlannerMention);
    const [isMentionPickerOpen, setIsMentionPickerOpen] = useState(false);
    // Where the typed MENTION_TRIGGER sits, so picking a collection takes it out of the text, and where the caret goes back to.
    const mentionTriggerIndexRef = useRef<number | null>(null);
    const caretAfterPickerRef = useRef<number | null>(null);

    const isWorking = phase === "working";
    // Until the first plan is there, there is nothing yet to ask the planner about.
    const isReadingSchema = isWorking && chatId === null && !hasProposal;
    const canSend = canSendMessage && draft.trim() !== "";
    const setDraft = (value: string) => setPlannerDraft(chatId, value);

    // Typing MENTION_TRIGGER where a word starts opens the picker, which adds the collection as a chip. Pasted text,
    // deleting, or backing out of the picker leave the MENTION_TRIGGER as written.
    const changeDraft = (event: ChangeEvent<HTMLTextAreaElement>) => {
        const { value, selectionStart } = event.target;
        const input = event.nativeEvent as Partial<InputEvent>;
        const isTriggerTyped = input.inputType === "insertText" && input.data === MENTION_TRIGGER;
        const isWordStart = selectionStart === 1 || /\s/.test(value.charAt(selectionStart - 2));

        setDraft(value);

        if (!isBusy && isTriggerTyped && isWordStart) {
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
            setDraft(draft.slice(0, triggerIndex) + draft.slice(triggerIndex + 1));
            caretAfterPickerRef.current = triggerIndex;
        }

        addPlannerMention(chatId, collection);
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
                            onRemove={() => removePlannerMention(chatId, collection)}
                        />
                    ))}
                </InputGroupAddon>
            )}
            <InputGroupTextarea
                ref={composerRef}
                rows={1}
                aria-label={chatId === null ? "Message for the planner" : `Message for the ${chatId} agent`}
                placeholder={describePlaceholder({ chatId, isWorking, isReadingSchema, mentions })}
                value={draft}
                disabled={isBusy || isReadingSchema}
                className="max-h-48 min-h-10 px-3.5"
                onChange={changeDraft}
                onKeyDown={(event) => {
                    if (isSendMessageKey(event)) {
                        event.preventDefault();

                        if (canSend) {
                            void session.sendDraft(chatId);
                        }
                    }
                }}
            />
            <InputGroupAddon align="block-end" className="gap-2 pt-0">
                {isWorking ? (
                    <Button
                        variant="secondary"
                        size="icon-sm"
                        className="ml-auto"
                        aria-label={chatId === null ? "Stop the planner" : `Stop the ${chatId} agent`}
                        onClick={() => void session.stop(chatId)}
                    >
                        <Square className="fill-current" aria-hidden="true" />
                    </Button>
                ) : (
                    canSendMessage && (
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
                                onClick={() => void session.sendDraft(chatId)}
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
    chatId,
    isWorking,
    isReadingSchema,
    mentions,
}: {
    chatId: PlannerChatId;
    isWorking: boolean;
    isReadingSchema: boolean;
    mentions: string[];
}): string {
    if (isReadingSchema) {
        return "The planner is working on a first plan.";
    }

    if (isWorking) {
        return `${chatId === null ? "The planner" : "The area agent"} is working. Stop it to send a new message.`;
    }

    if (mentions.length > 0) {
        return describeMentionPlaceholder(mentions);
    }

    return chatId === null
        ? `Ask the planner about the plan. Type ${MENTION_TRIGGER} to mention a collection.`
        : `Ask the ${chatId} agent for a change. Type ${MENTION_TRIGGER} to mention a collection.`;
}

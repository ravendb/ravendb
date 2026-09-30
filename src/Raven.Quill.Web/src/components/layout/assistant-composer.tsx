import { useEffect } from "react";
import { useForm, useWatch } from "react-hook-form";
import { Send, Square } from "lucide-react";
import { useAssistantChatStore } from "@/components/layout/assistant-chat-store";
import { useAssistantStore } from "@/components/layout/assistant-store";
import { FormTextarea } from "@/components/form/form-textarea";
import { Button } from "@/components/shadcn/ui/button";
import { Text } from "@/components/typography";

type PromptFormData = {
    prompt: string;
};

export function AssistantComposer() {
    const sendPrompt = useAssistantChatStore((state) => state.sendPrompt);
    const stopStreaming = useAssistantChatStore((state) => state.stopStreaming);
    const isStreaming = useAssistantChatStore((state) => state.isStreaming);
    const openCount = useAssistantStore((state) => state.openCount);

    const form = useForm<PromptFormData>({ defaultValues: { prompt: "" } });
    const prompt = useWatch({ control: form.control, name: "prompt" });
    const canSend = prompt.trim() !== "" && !isStreaming;

    // DOM side effect with no event to hang it on: opening the panel puts the caret in the
    // composer. The count starts at zero, so a panel restored as open on load takes no focus.
    useEffect(() => {
        if (openCount > 0) {
            form.setFocus("prompt");
        }
    }, [openCount, form]);

    // Deliberately not RHF's handleSubmit: FormTextarea disables itself while `formState.isSubmitting`
    // is set, and handleSubmit publishes that flag before it awaits — so the textarea would render
    // disabled, lose the focus it had, and never get the caret back for the next message. A non-empty
    // prompt is the only rule here, and `canSend` already applies it.
    function send() {
        if (!canSend) {
            return;
        }

        const trimmedPrompt = prompt.trim();
        form.reset();
        form.setFocus("prompt");
        void sendPrompt(trimmedPrompt);
    }

    return (
        <form
            className="border-t p-3"
            onSubmit={(event) => {
                event.preventDefault();
                send();
            }}
        >
            <div className="relative">
                <FormTextarea
                    control={form.control}
                    name="prompt"
                    aria-label="Message the AI assistant"
                    placeholder="Ask the AI assistant…"
                    rows={2}
                    textareaClassName="max-h-40 min-h-12 resize-none pr-12"
                    onKeyDown={(event) => {
                        if (event.key === "Enter" && !event.shiftKey) {
                            event.preventDefault();
                            send();
                        }
                    }}
                />
                {isStreaming ? (
                    <Button
                        type="button"
                        size="icon-sm"
                        variant="secondary"
                        onClick={stopStreaming}
                        className="absolute right-2 bottom-2"
                        aria-label="Stop answering"
                        title="Stop answering"
                    >
                        <Square aria-hidden="true" />
                    </Button>
                ) : (
                    <Button
                        type="submit"
                        size="icon-sm"
                        disabled={!canSend}
                        className="absolute right-2 bottom-2"
                        aria-label="Send message"
                    >
                        <Send aria-hidden="true" />
                    </Button>
                )}
            </div>
            <Text variant="caption" as="div" className="pt-2 text-center">
                Responses are AI-generated and may require verification.
            </Text>
        </form>
    );
}

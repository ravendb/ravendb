import { useEffect } from "react";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { Send } from "lucide-react";
import { nextAssistantMessageId, useAssistantChatStore } from "@/components/layout/assistant-chat-store";
import { useAssistantStore } from "@/components/layout/assistant-store";
import { FormTextarea } from "@/components/form/form-textarea";
import { Button } from "@/components/shadcn/ui/button";

const promptFormSchema = z.object({
    prompt: z.string().trim().min(1),
});

type PromptFormData = z.infer<typeof promptFormSchema>;

// UI-only stand-in until the assistant backend is wired up.
const NOT_CONNECTED_REPLY =
    "I'm not connected to a backend yet. Once I am, I'll answer questions about your apps and data right here.";

export function AssistantComposer() {
    const appendMessages = useAssistantChatStore((state) => state.appendMessages);
    const openCount = useAssistantStore((state) => state.openCount);

    const form = useForm<PromptFormData>({
        resolver: zodResolver(promptFormSchema),
        defaultValues: { prompt: "" },
    });
    const prompt = useWatch({ control: form.control, name: "prompt" });
    const isPromptEmpty = prompt.trim() === "";

    // DOM side effect with no event to hang it on: opening the panel puts the caret in the
    // composer. The count starts at zero, so a panel restored as open on load takes no focus.
    useEffect(() => {
        if (openCount > 0) {
            form.setFocus("prompt");
        }
    }, [openCount, form]);

    function send({ prompt }: PromptFormData) {
        appendMessages([
            { id: nextAssistantMessageId(), role: "user", text: prompt },
            { id: nextAssistantMessageId(), role: "assistant", text: NOT_CONNECTED_REPLY },
        ]);
        form.reset();
        form.setFocus("prompt");
    }

    const submitPrompt = form.handleSubmit(send);

    return (
        <form className="border-t p-3" onSubmit={submitPrompt}>
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
                            // An empty prompt submits nothing rather than raising a validation error.
                            if (!isPromptEmpty) {
                                void submitPrompt();
                            }
                        }
                    }}
                />
                <Button
                    type="submit"
                    size="icon-sm"
                    disabled={isPromptEmpty}
                    className="absolute right-2 bottom-2"
                    aria-label="Send message"
                >
                    <Send aria-hidden="true" />
                </Button>
            </div>
        </form>
    );
}

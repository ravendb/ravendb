import { Bot, Send } from "lucide-react";
import { PagePanel } from "@/components/data/page-panel";
import { Button } from "@/components/shadcn/ui/button";
import { Textarea } from "@/components/shadcn/ui/textarea";
import { Heading, Text } from "@/components/typography";

export function AiPage() {
    return (
        <PagePanel>
            <div className="mx-auto flex h-[36rem] max-h-[80vh] w-full max-w-3xl flex-col overflow-hidden rounded-lg border bg-card">
                <header className="flex items-center gap-2 border-b px-4 py-3">
                    <Bot className="size-5 text-primary-strong" aria-hidden />
                    <Heading variant="label">AI Assistant</Heading>
                </header>

                <div className="flex flex-1 flex-col items-center justify-center gap-3 px-6 text-center">
                    <div className="flex size-12 items-center justify-center rounded-full bg-muted">
                        <Bot className="size-6 text-muted-foreground" aria-hidden />
                    </div>
                    <div className="max-w-sm space-y-1">
                        <Text variant="label">AI Assistant is coming soon</Text>
                    </div>
                </div>

                <div className="border-t p-3">
                    <div className="relative">
                        <Textarea disabled placeholder="Ask the AI assistant…" className="min-h-12 resize-none pr-12" />
                        <Button
                            type="button"
                            size="icon"
                            disabled
                            className="absolute right-2 bottom-2 size-8"
                            aria-label="Send"
                        >
                            <Send className="size-4" aria-hidden />
                        </Button>
                    </div>
                </div>
            </div>
        </PagePanel>
    );
}

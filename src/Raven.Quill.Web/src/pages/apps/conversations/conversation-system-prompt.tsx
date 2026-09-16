import { ScrollText } from "lucide-react";
import { CodeBlock, TranscriptDisclosure } from "@/pages/apps/conversations/transcript-disclosure";

export function ConversationSystemPrompt({ disclosureKey, content }: { disclosureKey: string; content: string }) {
    return (
        <TranscriptDisclosure disclosureKey={disclosureKey} icon={ScrollText} label="System prompt">
            <div className="border-t px-3 py-3">
                <CodeBlock value={content} className="border-none" />
            </div>
        </TranscriptDisclosure>
    );
}

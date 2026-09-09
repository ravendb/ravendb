import { SlidersHorizontal } from "lucide-react";
import type { ConversationParam } from "@/api/generated/server-api";
import { Parameters } from "@/components/data/parameters";
import { TranscriptDisclosure } from "@/pages/apps/conversations/transcript-disclosure";

export function ConversationParams({ disclosureKey, params }: { disclosureKey: string; params: ConversationParam[] }) {
    return (
        <TranscriptDisclosure disclosureKey={disclosureKey} icon={SlidersHorizontal} label="Parameters">
            <div className="border-t px-3 py-3">
                <Parameters
                    className="flex-wrap"
                    params={params.map((param) => ({ name: param.key, value: param.value }))}
                />
            </div>
        </TranscriptDisclosure>
    );
}

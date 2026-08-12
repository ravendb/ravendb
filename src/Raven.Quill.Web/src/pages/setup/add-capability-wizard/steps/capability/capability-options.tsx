import { MessageSquare, ScanText, Sparkle } from "lucide-react";
import type { RadioCardOption } from "@/components/form/form-radio-cards";

export const CAPABILITY_OPTIONS: RadioCardOption<"agent" | "embeddings" | "genai">[] = [
    {
        value: "agent",
        label: "AI Agent",
        description:
            "Conversational agent grounded in live CDC data. System prompt + RQL tools. Deploy your agent to chosen channels.",
        icon: <MessageSquare className="size-5" />,
    },
    {
        value: "embeddings",
        label: "Embeddings generation",
        description: "Vector index over CDC data. Collection + field selection for vectorisation.",
        disabled: true,
        icon: <ScanText className="size-5" />,
    },
    {
        value: "genai",
        label: "GenAI",
        description: "Analyze and enrich your documents using an LLM.",
        disabled: true,
        icon: <Sparkle className="size-5" />,
    },
];

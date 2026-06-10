import { MessageSquare, ScanText, Sparkle } from "lucide-react";

type CapabilityOption = {
    value: "agent" | "embeddings" | "genai";
    label: string;
    description: string;
    icon: React.ReactNode;
    isDisabled?: boolean;
};

export const CAPABILITY_OPTIONS: CapabilityOption[] = [
    {
        value: "agent",
        label: "AI Agent",
        description:
            "Conversational agent grounded in live CDC data. System prompt + RQL tools. Deploy your agent to chosen channels.",
        icon: <MessageSquare className="mb-5 size-5" />,
    },
    {
        value: "embeddings",
        label: "Embeddings generation",
        description: "Vector index over CDC data. Collection + field selection for vectorisation.",
        isDisabled: true,
        icon: <ScanText className="mb-5 size-5" />,
    },
    {
        value: "genai",
        label: "GenAI",
        description: "Conversational agent grounded in live CDC data. System prompt + RQL tools.",
        isDisabled: true,
        icon: <Sparkle className="mb-5 size-5" />,
    },
];

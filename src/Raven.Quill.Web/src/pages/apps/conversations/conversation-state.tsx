import { cn } from "@/lib/utils";

// Maps a conversation state to its status-dot color. Unknown states fall back to muted so the UI
// degrades gracefully if the backend introduces a new state.
const STATE_DOT_CLASS: Record<string, string> = {
    active: "bg-emerald-500",
    idle: "bg-amber-500",
    completed: "bg-muted-foreground/40",
    closed: "bg-muted-foreground/40",
};

function getStateDotClass(state: string): string {
    return STATE_DOT_CLASS[state.toLowerCase()] ?? "bg-muted-foreground/40";
}

export function ConversationStateDot({ state, className }: { state: string; className?: string }) {
    return (
        <span className={cn("size-2 shrink-0 rounded-full", getStateDotClass(state), className)} aria-hidden="true" />
    );
}

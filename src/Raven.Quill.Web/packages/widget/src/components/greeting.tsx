import type { WidgetSuggestedPromptsLayout } from "@/widget-theme";

type GreetingProps = {
    title: string | null;
    body: string | null;
    suggestedPrompts: string[];
    layout: WidgetSuggestedPromptsLayout;
    isDisabled: boolean;
    onSelectPrompt: (prompt: string) => void;
};

// Stacked gives every prompt its own line, which is what a sentence-shaped prompt needs to stay readable.
// Inline flows them as chips and wraps, which only works when they are short, so it is the opt-in.
const LIST_CLASS: Record<WidgetSuggestedPromptsLayout, string> = {
    Stacked: "flex flex-col items-start gap-2",
    Inline: "flex flex-row flex-wrap gap-2",
};

// A stacked row spans the list so its button starts at the same edge every time; an inline chip must shrink
// to its own text or the row could never fit more than one.
const ITEM_CLASS: Record<WidgetSuggestedPromptsLayout, string> = {
    Stacked: "w-full",
    Inline: "",
};

export function Greeting({ title, body, suggestedPrompts, layout, isDisabled, onSelectPrompt }: GreetingProps) {
    return (
        <div className="flex min-h-full flex-col justify-center gap-[var(--rq-gap)] py-8">
            <div className="grid gap-2">
                {title !== null && title.length > 0 && <h1 className="text-lg font-semibold">{title}</h1>}
                {body !== null && body.length > 0 && <p className="text-rq-muted text-sm leading-relaxed">{body}</p>}
            </div>

            {suggestedPrompts.length > 0 && (
                <ul className={LIST_CLASS[layout]}>
                    {suggestedPrompts.map((prompt) => (
                        <li key={prompt} className={ITEM_CLASS[layout]}>
                            <button
                                type="button"
                                disabled={isDisabled}
                                onClick={() => onSelectPrompt(prompt)}
                                className="rounded-rq-pill border-rq-border bg-rq-surface hover:border-rq-accent focus-visible:ring-rq-accent border px-3 py-1.5 text-start text-[0.8125rem] transition-colors focus-visible:ring-2 focus-visible:outline-none disabled:opacity-50"
                            >
                                {prompt}
                            </button>
                        </li>
                    ))}
                </ul>
            )}
        </div>
    );
}

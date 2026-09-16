type GreetingProps = {
    title: string | null;
    body: string | null;
    suggestedPrompts: string[];
    isDisabled: boolean;
    onSelectPrompt: (prompt: string) => void;
};

export function Greeting({ title, body, suggestedPrompts, isDisabled, onSelectPrompt }: GreetingProps) {
    return (
        <div className="flex min-h-full flex-col justify-center gap-[var(--rq-gap)] py-8">
            <div className="grid gap-2">
                {title !== null && title.length > 0 && <h1 className="text-lg font-semibold">{title}</h1>}
                {body !== null && body.length > 0 && <p className="text-rq-muted text-sm leading-relaxed">{body}</p>}
            </div>

            {suggestedPrompts.length > 0 && (
                <ul className="flex flex-col items-start gap-2">
                    {suggestedPrompts.map((prompt) => (
                        <li key={prompt} className="w-full">
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

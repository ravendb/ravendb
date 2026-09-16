const DOT_DELAYS = ["0ms", "160ms", "320ms"];

export function ThinkingIndicator({ label = "Thinking" }: { label?: string }) {
    return (
        <div className="text-rq-muted flex items-center gap-2 text-sm">
            <span aria-hidden="true" className="flex items-center gap-1">
                {DOT_DELAYS.map((delay) => (
                    <span
                        key={delay}
                        style={{ animationDelay: delay }}
                        className="size-1.5 animate-bounce rounded-full bg-current"
                    />
                ))}
            </span>
            {label}
        </div>
    );
}

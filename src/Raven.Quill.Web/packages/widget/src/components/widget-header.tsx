type WidgetHeaderProps = {
    title: string;
    subtitle: string | null;
    initials: string | null;
};

/** Falls back to the title's initials so an operator who sets a name but no initials still gets an avatar. */
function deriveInitials(title: string): string {
    const words = title.trim().split(/\s+/).filter(Boolean);
    if (words.length === 0) return "AI";
    return words
        .slice(0, 2)
        .map((word) => word[0]!.toUpperCase())
        .join("");
}

export function WidgetHeader({ title, subtitle, initials }: WidgetHeaderProps) {
    const avatar = (initials?.trim() || deriveInitials(title)).slice(0, 3);

    return (
        <header className="border-rq-border flex shrink-0 items-center gap-3 border-b px-[var(--rq-pad-x)] py-[var(--rq-pad-y)]">
            <span
                aria-hidden="true"
                className="bg-rq-accent text-rq-accent-fg flex size-9 shrink-0 items-center justify-center rounded-full text-[13px] font-semibold"
            >
                {avatar}
            </span>
            <span className="min-w-0">
                <span className="block truncate text-sm font-semibold">{title}</span>
                {subtitle !== null && subtitle.length > 0 && (
                    <span className="text-rq-muted block truncate text-xs">{subtitle}</span>
                )}
            </span>
        </header>
    );
}

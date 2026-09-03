type WidgetHeaderProps = {
    title: string;
    subtitle: string | null;
    logo: string | null;
};

export function WidgetHeader({ title, subtitle, logo }: WidgetHeaderProps) {
    return (
        <header className="border-rq-border flex shrink-0 items-center gap-3 border-b px-[var(--rq-pad-x)] py-[var(--rq-pad-y)]">
            {logo !== null && logo.length > 0 && (
                <img src={logo} alt="" className="rounded-rq-logo size-9 shrink-0 object-contain" />
            )}
            <span className="min-w-0">
                <span className="block truncate text-sm font-semibold">{title}</span>
                {subtitle !== null && subtitle.length > 0 && (
                    <span className="text-rq-muted block truncate text-xs">{subtitle}</span>
                )}
            </span>
        </header>
    );
}

type PlaceholderPageProps = {
    title: string;
    description?: string;
};

export function PlaceholderPage({ title, description }: PlaceholderPageProps) {
    return (
        <div className="flex min-h-full w-full items-start">
            <section className="w-full rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
                <h2 className="text-base font-semibold tracking-normal">{title} placeholder</h2>
                {description && <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">{description}</p>}
            </section>
        </div>
    );
}

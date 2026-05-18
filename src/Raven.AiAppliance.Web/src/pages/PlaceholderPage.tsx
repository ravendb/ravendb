type PlaceholderPageProps = {
  title: string;
  description?: string;
};

export function PlaceholderPage({ title, description }: PlaceholderPageProps) {
  return (
    <div className="mx-auto flex min-h-[calc(100svh-7rem)] w-full max-w-5xl items-start">
      <section className="w-full rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
        <p className="text-sm font-medium text-primary">{title}</p>
        <h1 className="mt-2 text-2xl font-semibold tracking-normal">
          {title} coming soon
        </h1>
        {description && (
          <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">
            {description}
          </p>
        )}
      </section>
    </div>
  );
}

import type { ReactNode } from "react";

/**
 * One shape for every group on the page: heading and description outside, content in a bordered
 * panel. A `Card` puts its title inside the panel, which left the editable groups looking unlike the
 * read-only one directly beneath them for no reason the operator could act on.
 *
 * The panel carries no fill of its own. Card fill sits at 1.03:1 against the page here, so it was
 * never what told the two apart - the lock, the column headers and the presence of controls do that.
 */
export function LogSettingsGroup({
    id,
    title,
    description,
    action,
    footer,
    children,
}: {
    id: string;
    title: ReactNode;
    description?: ReactNode;
    /** Sits opposite the heading, for a state that needs the operator's attention. */
    action?: ReactNode;
    /** Below the panel, for notes about the group as a whole. */
    footer?: ReactNode;
    children: ReactNode;
}) {
    return (
        <section aria-labelledby={id} className="space-y-3">
            <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-2">
                <div className="space-y-1">
                    <h2 id={id} className="flex items-center gap-2 text-base font-medium">
                        {title}
                    </h2>
                    {description && <p className="max-w-prose text-sm text-muted-foreground">{description}</p>}
                </div>
                {action}
            </div>

            <div className="overflow-hidden rounded-lg border">{children}</div>

            {footer}
        </section>
    );
}

import type { ReactNode } from "react";

// Layout for an editable channel-detail "fill" tab: a fixed header (title, description, and the
// edit/save actions) that stays put while the body scrolls beneath it. Render inside a
// <form className="flex min-h-0 flex-1 flex-col"> so the header and scroller share the tab's height.
export function EditableTabShell({
    title,
    description,
    actions,
    children,
}: {
    title: string;
    description: string;
    actions: ReactNode;
    children: ReactNode;
}) {
    return (
        <>
            <div className="flex items-start justify-between gap-3 pt-5 pb-4">
                <div className="space-y-0.5">
                    <h2 className="text-lg font-semibold tracking-tight">{title}</h2>
                    <p className="text-sm text-muted-foreground">{description}</p>
                </div>
                {actions}
            </div>
            {/* -mx-2/px-2 keeps card borders and focus rings off the scroller's clip edge. */}
            <div className="-mx-2 min-h-0 flex-1 overflow-y-auto px-2 pb-5">{children}</div>
        </>
    );
}

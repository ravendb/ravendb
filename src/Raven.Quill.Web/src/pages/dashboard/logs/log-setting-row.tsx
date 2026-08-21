import type { ReactNode } from "react";
import { Field, FieldContent, FieldDescription, FieldTitle } from "@/components/shadcn/ui/field";

/**
 * A settings row that is not a form control: the label and description on the left, whatever states
 * the current value on the right. Mirrors the layout `FormInput`/`FormToggleGroup` render at
 * `orientation="responsive"`, so a row reads the same whether the page is being viewed or edited.
 */
export function LogSettingRow({
    label,
    description,
    note,
    children,
}: {
    label: ReactNode;
    description?: ReactNode;
    /** Sits below the description, for something about this row that is not part of its explanation. */
    note?: ReactNode;
    children?: ReactNode;
}) {
    return (
        <Field orientation="responsive">
            <FieldContent>
                <FieldTitle>{label}</FieldTitle>
                {/* FieldDescription tightens itself by 4px via `nth-last-2:-mt-1`, which fires only on
                    the rows that also carry a note - so a row with one would sit closer to its title
                    than a row without. Pinned, to keep every row on the same gap. */}
                {description && <FieldDescription className="mt-0!">{description}</FieldDescription>}
                {note}
            </FieldContent>
            {children}
        </Field>
    );
}

/**
 * A compact inline note about one row - the shape of an alert, at the size of a footnote.
 *
 * Laid out as a flex box rather than with `Alert`: everything inside is centred against everything
 * else, so the icon, the text and the action align by construction. Alert's grid puts a third child
 * on a second line, and a link set inline in the description never sat on the paragraph's baseline -
 * it is `inline-flex`, which synthesises its baseline from its own centred content.
 */
export function LogSettingNote({ children }: { children: ReactNode }) {
    return (
        <div className="mt-2 flex w-fit items-center gap-1 rounded-md border bg-muted/50 py-1 pr-2 pl-2.5 text-xs">
            {children}
        </div>
    );
}

/**
 * The read-only counterpart of a control. It borrows the control's footprint - same height, radius
 * and border - so a row does not visibly reflow when editing starts and the value never reads as
 * text stranded against the trailing edge.
 *
 * Not a disabled input: shadcn dims those to `opacity-50`, disabled controls are exempt from the
 * contrast rules so they tend to end up illegible, and their text cannot be selected - which for a
 * log path is the one thing an operator wants to do with it. This keeps full contrast and stays
 * selectable.
 */
export function LogSettingValue({ children }: { children: ReactNode }) {
    // The filled surface is what separates this from the editable input it stands in for: same border,
    // height and radius, but an input's fill is transparent and this one is not.
    return (
        <div className="flex min-h-8 min-w-0 items-center rounded-lg border border-input bg-muted px-2.5 py-1 text-sm font-medium break-all">
            {children}
        </div>
    );
}

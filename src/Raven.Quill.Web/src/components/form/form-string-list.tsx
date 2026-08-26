// react-hook-form's formState is a mutable proxy; compiler memoization would freeze
// the array-level error message read below.
"use no memo";

import {
    DndContext,
    KeyboardSensor,
    PointerSensor,
    closestCenter,
    useSensor,
    useSensors,
    type DragEndEvent,
} from "@dnd-kit/core";
import { restrictToParentElement, restrictToVerticalAxis } from "@dnd-kit/modifiers";
import {
    SortableContext,
    sortableKeyboardCoordinates,
    useSortable,
    verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { GripVertical, Plus, Trash2 } from "lucide-react";
import type { ReactNode } from "react";
import {
    get,
    useFieldArray,
    useFormState,
    type ArrayPath,
    type FieldArray,
    type FieldError,
    type FieldPath,
    type FieldValues,
    type UseFieldArrayProps,
} from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Text } from "@/components/typography";

type FormStringListProps<TFieldValues extends FieldValues, TName extends ArrayPath<TFieldValues>> = UseFieldArrayProps<
    TFieldValues,
    TName
> & {
    addButtonLabel: string;
    defaultValue: FieldArray<TFieldValues, TName>;
    description?: string;
    disabled?: boolean;
    emptyLabel?: string;
    fieldName: (index: number) => FieldPath<TFieldValues>;
    itemLabel?: (index: number) => string;
    label: string;
    placeholder?: string;
    /** Opt in: most string lists here are unordered sets, where a handle would suggest an order that
     *  carries no meaning. Turn it on only where the order is the point. */
    sortable?: boolean;
};

type RowProps = {
    id: string;
    children: ReactNode;
    handleLabel: string;
    isDisabled: boolean;
    /** Whether this row is inside a sortable list at all, i.e. whether it should register with dnd-kit. */
    isSortable: boolean;
    /** Whether this particular row should show its handle. False for the only row in a sortable list of one. */
    hasHandle: boolean;
};

/** One row of the list. A sortable row leads with its handle, so the grid grows a column; a plain row is
 *  exactly the two-column row this component has always rendered. */
function Row({ id, children, handleLabel, isDisabled, isSortable, hasHandle }: RowProps) {
    const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
        id,
        disabled: isSortable === false || isDisabled,
    });

    return (
        <div
            ref={setNodeRef}
            style={{ transform: CSS.Translate.toString(transform), transition }}
            className={
                hasHandle
                    ? "group grid gap-2 md:grid-cols-[auto_1fr_auto]"
                    : isSortable
                      ? "group grid gap-2 md:grid-cols-[1fr_auto]"
                      : "grid gap-2 md:grid-cols-[1fr_auto]"
            }
            data-dragging={isDragging || undefined}
        >
            {hasHandle && (
                <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    className="cursor-grab self-end text-muted-foreground group-data-[dragging]:cursor-grabbing"
                    disabled={isDisabled}
                    aria-label={handleLabel}
                    title={handleLabel}
                    {...attributes}
                    {...listeners}
                >
                    <GripVertical className="size-4" aria-hidden />
                </Button>
            )}
            {children}
        </div>
    );
}

export function FormStringList<TFieldValues extends FieldValues, TName extends ArrayPath<TFieldValues>>({
    addButtonLabel,
    control,
    defaultValue,
    description,
    disabled,
    emptyLabel = "No values.",
    fieldName,
    itemLabel,
    label,
    name,
    placeholder,
    sortable = false,
}: FormStringListProps<TFieldValues, TName>) {
    const fieldArray = useFieldArray({
        control,
        name,
    });

    const { errors } = useFormState({ control, name: name as unknown as FieldPath<TFieldValues> });
    const error = get(errors, name) as (FieldError & { root?: FieldError }) | undefined;
    const errorMessage = error?.message ?? error?.root?.message;

    const sensors = useSensors(
        useSensor(PointerSensor),
        useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
    );

    // A single row has nowhere to move to, so the handle column would be a control that does nothing.
    // This can change freely as rows are added or removed: it only toggles the handle inside a row that
    // is already mounted, not the wrapper element type around the whole list.
    const hasHandles = sortable && fieldArray.fields.length > 1;

    const handleDragEnd = (event: DragEndEvent) => {
        const { active, over } = event;
        if (over === null || active.id === over.id) return;

        const from = fieldArray.fields.findIndex((field) => field.id === active.id);
        const to = fieldArray.fields.findIndex((field) => field.id === over.id);
        if (from === -1 || to === -1) return;

        // `move` reorders the registered fields rather than remounting them, so each row keeps its own
        // validation and dirty state across the move.
        fieldArray.move(from, to);
    };

    const rows = fieldArray.fields.map((field, index) => (
        <Row
            key={field.id}
            id={field.id}
            isSortable={sortable}
            hasHandle={hasHandles}
            isDisabled={disabled === true}
            handleLabel={`Reorder ${label.toLowerCase()} ${index + 1}`}
        >
            <FormInput
                control={control}
                name={fieldName(index)}
                label={itemLabel?.(index)}
                placeholder={placeholder}
                disabled={disabled}
            />
            <Button
                type="button"
                variant="ghost"
                size="icon"
                className="self-end text-destructive"
                disabled={disabled}
                onClick={() => fieldArray.remove(index)}
                aria-label="Remove value"
                title="Remove value"
            >
                <Trash2 className="size-4" aria-hidden />
            </Button>
        </Row>
    ));

    return (
        <Field>
            <div className="flex items-start justify-between gap-3">
                <div>
                    <FieldLabel>{label}</FieldLabel>
                    {description && <FieldDescription>{description}</FieldDescription>}
                </div>
                <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={disabled}
                    onClick={() => fieldArray.append(defaultValue)}
                >
                    <Plus className="size-4" aria-hidden />
                    {addButtonLabel}
                </Button>
            </div>

            {fieldArray.fields.length === 0 ? (
                <Text variant="muted" as="div" className="rounded-md border bg-background px-3 py-4 text-center">
                    {emptyLabel}
                </Text>
            ) : sortable ? (
                <DndContext
                    sensors={sensors}
                    collisionDetection={closestCenter}
                    modifiers={[restrictToVerticalAxis, restrictToParentElement]}
                    onDragEnd={handleDragEnd}
                >
                    <SortableContext
                        items={fieldArray.fields.map((field) => field.id)}
                        strategy={verticalListSortingStrategy}
                    >
                        <div className="grid gap-2">{rows}</div>
                    </SortableContext>
                </DndContext>
            ) : (
                <div className="grid gap-2">{rows}</div>
            )}
            {errorMessage && <FieldDescription className="text-destructive">{errorMessage}</FieldDescription>}
        </Field>
    );
}

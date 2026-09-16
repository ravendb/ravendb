/** Form item shape used by FormStringList. Plain string arrays are not supported by
 * react-hook-form's useFieldArray, so string lists are stored as objects in form data. */
export type StringValueItem = { value: string };

export function toStringValueItems(values: readonly string[] | null | undefined): StringValueItem[] {
    return (values ?? []).map((value) => ({ value }));
}

export function toStringValues(items: readonly StringValueItem[] | null | undefined): string[] {
    return (items ?? []).map((item) => item.value.trim()).filter(Boolean);
}

export function withNestedSubmit<T>(action: (...args: T[]) => void) {
    return (e: React.SubmitEvent<HTMLFormElement>) => {
        e.preventDefault();
        e.stopPropagation();
        action();
    };
}

export function getOptionLabel<TValue extends string>(
    options: ReadonlyArray<{ value: TValue; label: React.ReactNode }>,
    value: TValue | null | undefined,
): React.ReactNode {
    return options.find((option) => option.value === value)?.label;
}

export function preventEnterKeySubmission(e: React.KeyboardEvent<HTMLFormElement>) {
    const target = e.target;
    if (e.key === "Enter" && target instanceof HTMLInputElement) {
        e.preventDefault();
    }
}

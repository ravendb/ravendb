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

/**
 * Enter without Shift sends a chat message, but not while an IME composes: there Enter confirms the
 * composition, and Safari reports that keydown after the composition ended, as keyCode 229.
 */
export function isSendMessageKey(event: React.KeyboardEvent): boolean {
    return (
        event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing && event.nativeEvent.keyCode !== 229
    );
}

export function preventEnterKeySubmission(e: React.KeyboardEvent<HTMLFormElement>) {
    const target = e.target;
    if (e.key === "Enter" && target instanceof HTMLInputElement) {
        e.preventDefault();
    }
}

export function withNestedSubmit<T>(action: (...args: T[]) => void) {
    return (e: React.SubmitEvent<HTMLFormElement>) => {
        e.preventDefault();
        e.stopPropagation();
        action();
    };
}

export function getOptionLabel<TValue extends string>(
    options: ReadonlyArray<{ value: TValue; label: string }>,
    value: TValue | null | undefined,
): string | undefined {
    return options.find((option) => option.value === value)?.label;
}

export function preventEnterKeySubmission(e: React.KeyboardEvent<HTMLFormElement>) {
    const target = e.target;
    if (e.key === "Enter" && target instanceof HTMLInputElement) {
        e.preventDefault();
    }
}

export function withNestedSubmit<T>(action: (...args: T[]) => void) {
    return (e: React.SubmitEvent<HTMLFormElement>) => {
        e.preventDefault();
        e.stopPropagation();
        action();
    };
}

export function preventEnterKeySubmission(e: React.KeyboardEvent<HTMLFormElement>) {
    const target = e.target;
    if (e.key === "Enter" && target instanceof HTMLInputElement) {
        e.preventDefault();
    }
}

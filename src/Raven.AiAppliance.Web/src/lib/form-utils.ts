export function withNestedSubmit<T>(action: (...args: T[]) => void) {
    return (e: React.SubmitEvent<HTMLFormElement>) => {
        e.preventDefault();
        e.stopPropagation();
        action();
    };
}

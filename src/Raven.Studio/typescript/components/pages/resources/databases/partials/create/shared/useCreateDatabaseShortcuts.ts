import { useCallback, useEffect } from "react";

interface useCreateDatabaseShortcutsProps {
    submit: () => Promise<void>;
    handleGoNext: () => Promise<void>;
    isLastStep: boolean;
    canQuickCreate?: boolean;
}

export function useCreateDatabaseShortcuts({
    submit,
    handleGoNext,
    isLastStep,
    canQuickCreate,
}: useCreateDatabaseShortcutsProps) {
    // Enter in the last step submits the form
    // Enter in an earlier step goes to the next step
    // if canQuickCreate is true, (ctrl + Enter) submits the form
    const handleKeyPress = useCallback(
        async (event: KeyboardEvent) => {
            if (event.key !== "Enter") {
                return;
            }

            event.preventDefault();

            if (isLastStep || (canQuickCreate && event.ctrlKey)) {
                await submit();
            } else {
                await handleGoNext();
            }
        },
        [handleGoNext, submit, isLastStep, canQuickCreate]
    );

    useEffect(() => {
        document.addEventListener("keydown", handleKeyPress);
        return () => {
            document.removeEventListener("keydown", handleKeyPress);
        };
    }, [handleKeyPress]);
}

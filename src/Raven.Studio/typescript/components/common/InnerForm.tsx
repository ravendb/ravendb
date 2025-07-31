import useUniqueId from "components/hooks/useUniqueId";
import { PropsWithChildren, useCallback, useEffect } from "react";

// To submit button use onClick={handleSubmit}

interface InnerFormProps {
    children: React.ReactNode;
    onSubmit: () => void | Promise<void>;
    className?: string;
}

export default function InnerForm({ children, onSubmit, className }: PropsWithChildren<InnerFormProps>) {
    const id = useUniqueId("inner-form-");

    const handleKeyPress = useCallback(async (e: KeyboardEvent) => {
        const isChildOfInnerForm =
            e.target instanceof HTMLInputElement && document.getElementById(id)?.contains(e.target);

        if (e.key === "Enter" && !e.shiftKey && isChildOfInnerForm) {
            e.preventDefault();
            await onSubmit();
        }
    }, []);

    useEffect(() => {
        document.addEventListener("keydown", handleKeyPress);
        return () => {
            document.removeEventListener("keydown", handleKeyPress);
        };
    }, [handleKeyPress]);

    return (
        <div id={id} className={className}>
            {children}
        </div>
    );
}

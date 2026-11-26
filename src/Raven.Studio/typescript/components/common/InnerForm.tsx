import { KeyboardEvent, PropsWithChildren } from "react";

// To submit button use onClick={handleSubmit}

interface InnerFormProps {
    children: React.ReactNode;
    onSubmit: () => void | Promise<void>;
    className?: string;
}

export default function InnerForm({ children, onSubmit, className }: PropsWithChildren<InnerFormProps>) {
    const submitOnEnter = async (e: KeyboardEvent<HTMLDivElement>) => {
        const isOwnInput = e.target instanceof HTMLInputElement && e.currentTarget.contains(e.target);

        if (e.key === "Enter" && !e.shiftKey && isOwnInput) {
            e.preventDefault();
            await onSubmit();
        }
    };

    return (
        <div className={className} onKeyDown={submitOnEnter}>
            {children}
        </div>
    );
}

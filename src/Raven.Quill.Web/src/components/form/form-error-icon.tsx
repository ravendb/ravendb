// React Compiler memoization is disabled here: the message is derived from react-hook-form's
// mutable errors object, which keeps a stable identity across updates.
"use no memo";

import { useEffect, useEffectEvent } from "react";
import { type Control, type FieldPath, type FieldValues, useFormState } from "react-hook-form";
import { CircleAlert } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { cn } from "@/lib/utils";

type FormErrorIconProps<TFieldValues extends FieldValues> = {
    control: Control<TFieldValues>;
    paths: readonly FieldPath<TFieldValues>[];
    onError?: () => void;
    className?: string;
};

export function FormErrorIcon<TFieldValues extends FieldValues>({
    control,
    paths,
    onError,
    className,
}: FormErrorIconProps<TFieldValues>) {
    const { errors } = useFormState({ control, name: [...paths] });

    const message = paths.map((path) => findErrorMessage(getByPath(errors, path))).find(Boolean);
    const hasError = Boolean(message);

    const notifyError = useEffectEvent(() => onError?.());

    useEffect(() => {
        if (hasError) {
            notifyError();
        }
    }, [hasError]);

    if (!message) {
        return null;
    }

    return (
        <TooltipProvider>
            <Tooltip>
                <TooltipTrigger asChild>
                    <span className={cn("inline-flex text-destructive", className)}>
                        <CircleAlert className="size-3.5" aria-hidden="true" />
                        <span className="sr-only">{message}</span>
                    </span>
                </TooltipTrigger>
                <TooltipContent>{message}</TooltipContent>
            </Tooltip>
        </TooltipProvider>
    );
}

function getByPath(target: unknown, path: string): unknown {
    let current: unknown = target;
    for (const key of path.split(".")) {
        if (current == null || typeof current !== "object") {
            return undefined;
        }
        current = (current as Record<string, unknown>)[key];
    }
    return current;
}

function findErrorMessage(error: unknown): string | undefined {
    if (error == null || typeof error !== "object") {
        return undefined;
    }

    const record = error as Record<string, unknown>;
    if (typeof record.message === "string" && record.message) {
        return record.message;
    }

    for (const [key, value] of Object.entries(record)) {
        // Field errors carry a DOM `ref` — nothing to search there.
        if (key === "ref") {
            continue;
        }
        const message = findErrorMessage(value);
        if (message) {
            return message;
        }
    }

    return undefined;
}

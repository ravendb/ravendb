import type { WizardError } from "@/api/generated/server-api";
import { ErrorDetails } from "@/components/data/error-details";
import { cn } from "@/lib/utils";

export function WizardErrorList({ errors, className }: { errors?: WizardError[]; className?: string }) {
    const visibleErrors = errors?.filter(Boolean) ?? [];

    if (visibleErrors.length === 0) {
        return null;
    }

    return (
        <ul className={cn("grid gap-2 text-sm text-destructive", className)}>
            {visibleErrors.map((error, index) => (
                <li key={index} className="grid gap-1">
                    <span className="whitespace-pre-wrap">{error.message}</span>
                    {error.details && <ErrorDetails details={error.details} />}
                </li>
            ))}
        </ul>
    );
}

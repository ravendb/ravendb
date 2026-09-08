import { CircleAlertIcon } from "lucide-react";
import type { WizardError } from "@/api/generated/server-api";
import { ErrorDetails } from "@/components/data/error-details";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";

/**
 * Page-level banner for the errors a wizard step's server call reported. `title` names what failed,
 * because the server messages below it describe the cause without saying which call produced them.
 */
export function WizardErrorList({
    errors,
    title,
    className,
}: {
    errors?: WizardError[];
    title: string;
    className?: string;
}) {
    const visibleErrors = errors?.filter(Boolean) ?? [];

    if (visibleErrors.length === 0) {
        return null;
    }

    return (
        <Alert variant="destructive" className={className}>
            <CircleAlertIcon aria-hidden="true" />
            <AlertTitle>{title}</AlertTitle>
            <AlertDescription>
                <ul className="grid gap-2">
                    {visibleErrors.map((error, index) => (
                        <li key={index} className="grid gap-1">
                            <span className="whitespace-pre-wrap">{error.message}</span>
                            {error.details && <ErrorDetails details={error.details} />}
                        </li>
                    ))}
                </ul>
            </AlertDescription>
        </Alert>
    );
}

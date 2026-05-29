import { CheckCircle2, OctagonAlert } from "lucide-react";
import type { ReactNode } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { cn } from "@/lib/utils";
import type { SetupWizardMessage } from "@/pages/setup/add-app-wizard/wizard-model";

export function StepSection({
    children,
    description,
    message,
    title,
}: {
    children: ReactNode;
    description?: string;
    message?: SetupWizardMessage;
    title: string;
}) {
    return (
        <section className="grid gap-5">
            <div>
                <h2 className="text-2xl font-semibold tracking-normal">{title}</h2>
                {description && <p className="mt-3 text-sm text-muted-foreground">{description}</p>}
            </div>
            {children}
            <StepMessageAlert message={message} />
        </section>
    );
}

function StepMessageAlert({ message }: { message?: SetupWizardMessage }) {
    if (!message) {
        return null;
    }

    const Icon = message.type === "success" ? CheckCircle2 : OctagonAlert;

    return (
        <Alert
            variant={message.type === "error" ? "destructive" : "default"}
            className={cn(
                message.type === "success" &&
                    "border-emerald-700/30 bg-emerald-950/20 text-foreground dark:bg-emerald-950/60",
            )}
        >
            <Icon className="size-4" aria-hidden="true" />
            <AlertTitle>{message.title}</AlertTitle>
            {message.description && <AlertDescription>{message.description}</AlertDescription>}
        </Alert>
    );
}

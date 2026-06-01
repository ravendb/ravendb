import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Alert } from "@/components/shadcn/ui/alert";
import type { PropsWithChildren } from "react";

export function StepSection({ children, title, description, error }: PropsWithChildren<WizardBodyComponentProps>) {
    return (
        <section className="grid gap-5">
            <div>
                <h2 className="text-2xl font-semibold tracking-normal">{title}</h2>
                {description && <p className="mt-3 text-sm text-muted-foreground">{description}</p>}
            </div>
            {children}
            {error && <Alert variant="destructive">{error.message}</Alert>}
        </section>
    );
}

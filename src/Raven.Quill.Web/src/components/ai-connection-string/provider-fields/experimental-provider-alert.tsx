import type { ReactNode } from "react";
import { TriangleAlertIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";

export function ExperimentalProviderAlert({ children }: { children: ReactNode }) {
    return (
        <Alert>
            <TriangleAlertIcon />
            <AlertTitle>Experimental provider</AlertTitle>
            <AlertDescription>{children}</AlertDescription>
        </Alert>
    );
}

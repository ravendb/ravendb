import type { ReactNode } from "react";
import { FieldLabel } from "@/components/shadcn/ui/field";

type FormFieldLabelProps = {
    htmlFor: string;
    label?: ReactNode;
    addon?: ReactNode;
};

export function FormFieldLabel({ htmlFor, label, addon }: FormFieldLabelProps) {
    if (label == null) {
        return null;
    }

    if (!addon) {
        return <FieldLabel htmlFor={htmlFor}>{label}</FieldLabel>;
    }

    return (
        <div className="flex items-center gap-1">
            <FieldLabel htmlFor={htmlFor}>{label}</FieldLabel>
            {addon}
        </div>
    );
}

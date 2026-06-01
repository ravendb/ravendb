import { Button } from "@/components/shadcn/ui/button";
import { StepSection } from "@/pages/setup/add-app-wizard/app-wizard-step-section";
import { useFormContext } from "react-hook-form";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { FormInput } from "@/components/form/form-input";

export function PreviewStep(props: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();

    return (
        <StepSection {...props}>
            <div className="grid gap-5">
                <FormInput control={control} name="preview.table" />
                <div className="flex justify-end">
                    <Button type="button" variant="secondary">
                        Run preview
                    </Button>
                </div>
            </div>
        </StepSection>
    );
}

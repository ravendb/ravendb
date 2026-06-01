/* eslint-disable react-refresh/only-export-components */
import { Button } from "@/components/shadcn/ui/button";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import { useFormContext } from "react-hook-form";
import type { AppFormData } from "@/pages/setup/add-app-wizard/wizard-model";
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

// function MappingPreviewResult({ result }: { result: TestMappingResponse | null }) {
//     if (!result) {
//         return (
//             <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
//                 Preview has not been run yet.
//             </div>
//         );
//     }

//     return (
//         <div className="grid gap-3">
//             <MessageList messages={[...result.errors, ...result.warnings]} tone="destructive" />
//             {result.results.length === 0 ? (
//                 <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
//                     No preview rows returned.
//                 </div>
//             ) : (
//                 result.results.map((row, index) => (
//                     <pre key={index} className="max-h-64 overflow-auto rounded-lg border bg-background p-3 text-xs">
//                         {row.error || row.document || row.sourceRow || "Empty result"}
//                     </pre>
//                 ))
//             )}
//         </div>
//     );
// }

// function SummaryPanel({ label, value }: { label: string; value: string }) {
//     return (
//         <div className="rounded-lg border bg-background p-4">
//             <p className="text-xs font-medium text-muted-foreground">{label}</p>
//             <p className="mt-2 truncate text-sm font-semibold">{value}</p>
//         </div>
//     );
// }

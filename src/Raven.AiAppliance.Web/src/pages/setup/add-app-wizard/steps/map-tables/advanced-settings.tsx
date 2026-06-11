import { useState, type ReactNode } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormAceEditor } from "@/components/form/form-ace-editor";
import { FormSwitch } from "@/components/form/form-switch";
import { Button } from "@/components/shadcn/ui/button";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { EmbeddedTablePath, RootTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

type AdvancedSettingsProps = {
    path: RootTablePath | EmbeddedTablePath;
    children?: ReactNode;
};

/** Collapsible section for the rarely used table options: transform patch and delete behavior. */
export function AdvancedSettings({ path, children }: AdvancedSettingsProps) {
    const { control } = useFormContext<AppFormData>();

    const patch = useWatch({ control, name: `${path}.patch` });
    const ignoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });
    const deletePatch = useWatch({ control, name: `${path}.onDelete.patch` });

    const hasAdvancedValues = Boolean(patch || ignoreDeletes || deletePatch);
    const [isExpanded, setIsExpanded] = useState(hasAdvancedValues);

    return (
        <div className="grid gap-3">
            <Button
                type="button"
                variant="ghost"
                size="sm"
                className="justify-self-start"
                onClick={() => setIsExpanded((expanded) => !expanded)}
            >
                {isExpanded ? (
                    <ChevronDown className="size-4" aria-hidden="true" />
                ) : (
                    <ChevronRight className="size-4" aria-hidden="true" />
                )}
                Advanced settings
            </Button>
            {isExpanded && (
                <div className="grid gap-4">
                    {children}
                    <FormAceEditor
                        control={control}
                        name={`${path}.patch`}
                        label="Patch script"
                        description="Optional script applied to each document before it is stored."
                        mode="javascript"
                        height="120px"
                    />
                    <FormSwitch control={control} name={`${path}.onDelete.ignoreDeletes`} label="Ignore deletes" />
                    <FormAceEditor
                        control={control}
                        name={`${path}.onDelete.patch`}
                        label="On-delete patch script"
                        description="Optional script applied when a source row is deleted."
                        mode="javascript"
                        height="120px"
                    />
                </div>
            )}
        </div>
    );
}

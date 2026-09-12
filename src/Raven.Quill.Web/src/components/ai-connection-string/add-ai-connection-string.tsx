import { useState, type ReactNode } from "react";
import { Plus } from "lucide-react";
import type { AiModelType } from "@/api/generated/server-api";
import { Button } from "@/components/shadcn/ui/button";
import { SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/shadcn/ui/sheet";
import { GuardedSheet } from "@/components/form/unsaved-changes/guarded-overlays";
import { AiConnectionStringForm } from "@/components/ai-connection-string/ai-connection-string-form";
import { getDefaultValues } from "@/components/ai-connection-string/ai-connection-string-utils";

type AddAiConnectionStringProps = {
    modelType: AiModelType;
    onCreated: (name: string) => void | Promise<void>;
    trigger?: ReactNode;
};

export function AddAiConnectionString({ modelType, onCreated, trigger }: AddAiConnectionStringProps) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <GuardedSheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>
                {trigger ?? (
                    <Button type="button" variant="secondary">
                        <Plus className="size-3.5" aria-hidden="true" />
                        Add connection string
                    </Button>
                )}
            </SheetTrigger>
            <SheetContent className="w-full gap-0 sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>Add connection string</SheetTitle>
                    <SheetDescription>Pick a provider and fill in the connection details.</SheetDescription>
                </SheetHeader>

                <AiConnectionStringForm
                    modelType={modelType}
                    defaultValues={getDefaultValues()}
                    isEditing={false}
                    onSaved={async (name) => {
                        await onCreated(name);
                        setIsOpen(false);
                    }}
                />
            </SheetContent>
        </GuardedSheet>
    );
}

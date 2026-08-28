import { useState, type ReactNode } from "react";
import { Text } from "@/components/typography";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { AiModelType } from "@/api/generated/server-api";
import { Button } from "@/components/shadcn/ui/button";
import { SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/shadcn/ui/sheet";
import { GuardedSheet } from "@/components/form/unsaved-changes/guarded-overlays";
import { AiConnectionStringForm } from "@/components/ai-connection-string/ai-connection-string-form";
import { mapDtoToFormData } from "@/components/ai-connection-string/ai-connection-string-utils";

type EditAiConnectionStringProps = {
    name: string;
    modelType: AiModelType;
    trigger: ReactNode;
    onSaved: (name: string) => void | Promise<void>;
};

export function EditAiConnectionString({ name, modelType, trigger, onSaved }: EditAiConnectionStringProps) {
    const [isOpen, setIsOpen] = useState(false);

    // Only fetch the full connection string once the sheet opens.
    const detailQuery = useQuery({ ...api.queries.aiConnectionStrings.detail(name), enabled: isOpen });

    return (
        <GuardedSheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>{trigger}</SheetTrigger>
            <SheetContent className="w-full gap-0 sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>Edit connection string</SheetTitle>
                    <SheetDescription>Update the provider details for “{name}”.</SheetDescription>
                </SheetHeader>

                {detailQuery.isPending ? (
                    <Text variant="muted" className="p-4">
                        Loading connection string…
                    </Text>
                ) : detailQuery.isError || !detailQuery.data ? (
                    <div className="space-y-3 p-4">
                        <Text variant="muted">Could not load the connection string.</Text>
                        <Button type="button" variant="outline" size="sm" onClick={() => void detailQuery.refetch()}>
                            Retry
                        </Button>
                    </div>
                ) : (
                    <AiConnectionStringForm
                        modelType={modelType}
                        defaultValues={mapDtoToFormData(detailQuery.data)}
                        isEditing
                        existingIdentifier={detailQuery.data.identifier ?? undefined}
                        onSaved={async (savedName) => {
                            await onSaved(savedName);
                            setIsOpen(false);
                        }}
                    />
                )}
            </SheetContent>
        </GuardedSheet>
    );
}

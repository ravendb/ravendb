import { useForm } from "react-hook-form";
import { SearchIcon } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import {
    SheetClose,
    SheetContent,
    SheetDescription,
    SheetFooter,
    SheetHeader,
    SheetTitle,
} from "@/components/shadcn/ui/sheet";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { FormStringList } from "@/components/form/form-string-list";
import { GuardedSheet } from "@/components/form/unsaved-changes/guarded-overlays";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { toStringValueItems, toStringValues, type StringValueItem } from "@/lib/form-utils";
import { normalizeDiscoverSchemas } from "@/pages/setup/add-app-wizard/steps/verify/use-discover-tables";

type DefineSchemasSheetProps = {
    isOpen: boolean;
    onOpenChange: (isOpen: boolean) => void;
    initialSchemas: string[];
    isDiscovering: boolean;
    /** Persists the schemas and re-runs discovery. Closing the sheet on success is the caller's job. */
    onSave: (schemas: string[]) => Promise<void>;
};

export function DefineSchemasSheet({
    isOpen,
    onOpenChange,
    initialSchemas,
    isDiscovering,
    onSave,
}: DefineSchemasSheetProps) {
    return (
        <GuardedSheet open={isOpen} onOpenChange={onOpenChange}>
            <SheetContent side="right">
                {/* Rendered only while open, so the draft form resets on each open. */}
                <DefineSchemasSheetBody initialSchemas={initialSchemas} isDiscovering={isDiscovering} onSave={onSave} />
            </SheetContent>
        </GuardedSheet>
    );
}

type DefineSchemasFormData = {
    schemas: StringValueItem[];
};

function DefineSchemasSheetBody({
    initialSchemas,
    isDiscovering,
    onSave,
}: Omit<DefineSchemasSheetProps, "isOpen" | "onOpenChange">) {
    const form = useForm<DefineSchemasFormData>({
        defaultValues: { schemas: toStringValueItems(initialSchemas) },
    });

    // No markSaved needed: the caller closes the sheet while the save is still in flight.
    useFormUnsavedChanges(form);

    const handleSave = form.handleSubmit(({ schemas }) => onSave(normalizeDiscoverSchemas(toStringValues(schemas))));

    return (
        <>
            <SheetHeader>
                <SheetTitle>Define schemas</SheetTitle>
                <SheetDescription>
                    Tables are discovered from the default schema of the configured connection. Add one or more schemas
                    to discover tables from those schemas instead.
                </SheetDescription>
            </SheetHeader>
            <div className="flex-1 overflow-y-auto px-4">
                <FormStringList
                    control={form.control}
                    name="schemas"
                    fieldName={(index) => `schemas.${index}.value`}
                    defaultValue={{ value: "" }}
                    label="Schemas"
                    addButtonLabel="Add schema"
                    placeholder="e.g. public"
                    emptyLabel="No schemas — the connection's default schema is used."
                />
            </div>
            <SheetFooter>
                <Button type="button" onClick={() => void handleSave()} disabled={isDiscovering}>
                    {isDiscovering ? <Spinner /> : <SearchIcon aria-hidden="true" />}
                    Save & discover
                </Button>
                {/* SheetClose, so the close goes through the guard. */}
                <SheetClose asChild>
                    <Button type="button" variant="outline" disabled={isDiscovering}>
                        Close
                    </Button>
                </SheetClose>
            </SheetFooter>
        </>
    );
}

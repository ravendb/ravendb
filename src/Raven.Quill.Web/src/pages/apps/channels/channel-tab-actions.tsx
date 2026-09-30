import { Pencil } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";

// The edit/cancel/save control shown next to an editable channel-tab's section header. The section is
// read-only until "Edit" is pressed, which swaps in "Cancel" (revert) and "Save" (submit the form).
export function SectionEditActions({
    isEditing,
    isSaving,
    onEdit,
    onCancel,
}: {
    isEditing: boolean;
    isSaving: boolean;
    onEdit: () => void;
    onCancel: () => void;
}) {
    if (!isEditing) {
        return (
            <Button type="button" size="sm" variant="outline" onClick={onEdit}>
                <Pencil aria-hidden="true" />
                Edit
            </Button>
        );
    }

    return (
        <div className="flex gap-2">
            <Button type="button" size="sm" variant="outline" onClick={onCancel} disabled={isSaving}>
                Cancel
            </Button>
            <Button type="submit" size="sm" disabled={isSaving}>
                {isSaving && <Spinner />}
                Save
            </Button>
        </div>
    );
}

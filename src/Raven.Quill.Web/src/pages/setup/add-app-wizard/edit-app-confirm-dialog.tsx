import type { ReactNode } from "react";
import { useNavigate } from "react-router";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { appRoutes } from "@/lib/app-routes";

export function EditAppConfirmDialog({ slug, trigger }: { slug: string; trigger: ReactNode }) {
    const navigate = useNavigate();

    return (
        <ConfirmDialog
            variant="warning"
            trigger={trigger}
            title="Edit this app’s configuration?"
            description="Changing an early step can invalidate the steps after it, so you’ll need to review the rest. Nothing is saved until you finish the wizard."
            confirmLabel="Edit configuration"
            onConfirm={() => navigate(appRoutes.editApp(slug))}
        />
    );
}

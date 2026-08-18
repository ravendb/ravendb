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
            title="Edit application configuration?"
            description="Changes on one step affect the steps after it: the connection determines which tables can be selected, and the table selection determines the mapping. Review the later steps after changing an earlier one."
            confirmLabel="Edit configuration"
            onConfirm={() => navigate(appRoutes.editApp(slug))}
        />
    );
}

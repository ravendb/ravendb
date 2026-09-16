import * as React from "react";
import { InfoIcon, OctagonAlertIcon, TriangleAlertIcon, type LucideIcon } from "lucide-react";

import { cn } from "@/lib/utils";
import {
    AlertDialog,
    AlertDialogAction,
    AlertDialogCancel,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
    AlertDialogTrigger,
} from "@/components/shadcn/ui/alert-dialog";

export type ConfirmVariant = "primary" | "warning" | "destructive";

const VARIANT_CONFIG: Record<
    ConfirmVariant,
    { buttonVariant: "default" | "warning" | "destructive"; iconWrap: string; Icon: LucideIcon }
> = {
    primary: { buttonVariant: "default", iconWrap: "bg-primary/10 text-primary-strong", Icon: InfoIcon },
    warning: { buttonVariant: "warning", iconWrap: "bg-warning/10 text-badge-warning-fg", Icon: TriangleAlertIcon },
    destructive: {
        buttonVariant: "destructive",
        iconWrap: "bg-destructive/10 text-destructive",
        Icon: OctagonAlertIcon,
    },
};

export type ConfirmDialogProps = {
    title: React.ReactNode;
    description?: React.ReactNode;
    trigger?: React.ReactNode;
    confirmLabel?: React.ReactNode;
    cancelLabel?: React.ReactNode;
    variant?: ConfirmVariant;
    icon?: React.ReactNode;
    onConfirm?: () => void;
    children?: React.ReactNode;
    open?: boolean;
    defaultOpen?: boolean;
    onOpenChange?: (open: boolean) => void;
};

export function ConfirmDialog({
    title,
    description,
    trigger,
    confirmLabel = "Confirm",
    cancelLabel = "Cancel",
    variant = "primary",
    icon,
    onConfirm,
    children,
    open,
    defaultOpen,
    onOpenChange,
}: ConfirmDialogProps) {
    const { buttonVariant, iconWrap, Icon } = VARIANT_CONFIG[variant];
    const iconNode = icon === undefined ? <Icon className="size-5" /> : icon;

    return (
        <AlertDialog open={open} defaultOpen={defaultOpen} onOpenChange={onOpenChange}>
            {trigger && <AlertDialogTrigger asChild>{trigger}</AlertDialogTrigger>}
            <AlertDialogContent>
                <AlertDialogHeader className="flex-row items-start gap-3">
                    {iconNode !== null && (
                        <span
                            className={cn("flex size-9 shrink-0 items-center justify-center rounded-full", iconWrap)}
                            aria-hidden="true"
                        >
                            {iconNode}
                        </span>
                    )}
                    <div className="flex flex-col gap-2">
                        <AlertDialogTitle>{title}</AlertDialogTitle>
                        {description && <AlertDialogDescription>{description}</AlertDialogDescription>}
                    </div>
                </AlertDialogHeader>
                {children}
                <AlertDialogFooter>
                    <AlertDialogCancel>{cancelLabel}</AlertDialogCancel>
                    <AlertDialogAction variant={buttonVariant} onClick={onConfirm}>
                        {confirmLabel}
                    </AlertDialogAction>
                </AlertDialogFooter>
            </AlertDialogContent>
        </AlertDialog>
    );
}

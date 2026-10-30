import { Fragment, type ReactNode } from "react";
import { ArrowLeft } from "lucide-react";
import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbLink,
    BreadcrumbList,
    BreadcrumbSeparator,
} from "@/components/shadcn/ui/breadcrumb";
import { Button } from "@/components/shadcn/ui/button";
import { usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";

type BreadcrumbStep = { label: string; onClick: () => void };

export function PlannerBreadcrumb({ items, children }: { items: BreadcrumbStep[]; children: ReactNode }) {
    const { openPlan } = usePlannerActions();

    return (
        <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
            <Breadcrumb>
                <BreadcrumbList>
                    {items.map((item) => (
                        <Fragment key={item.label}>
                            <BreadcrumbItem>
                                <BreadcrumbLink asChild>
                                    <button type="button" onClick={item.onClick}>
                                        {item.label}
                                    </button>
                                </BreadcrumbLink>
                            </BreadcrumbItem>
                            <BreadcrumbSeparator>/</BreadcrumbSeparator>
                        </Fragment>
                    ))}
                    <BreadcrumbItem>{children}</BreadcrumbItem>
                </BreadcrumbList>
            </Breadcrumb>
            <Button variant="outline" size="sm" onClick={openPlan}>
                <ArrowLeft data-icon="inline-start" />
                Back to plan
            </Button>
        </div>
    );
}

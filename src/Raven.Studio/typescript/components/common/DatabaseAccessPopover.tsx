import React, { PropsWithChildren, ReactNode } from "react";
import { Condition, ConditionalPopover, ConditionalPopoverProps } from "./ConditionalPopover";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { getDatabaseAccessRequiredMessage } from "components/utils/accessUtils";

interface DatabaseAdminAccessPopoverProps
    extends Required<PropsWithChildren>,
        Omit<ConditionalPopoverProps, "conditions"> {
    accessLevel?: databaseAccessLevel;
    conditions?: Condition | Condition[];
    customAccessRequiredMessage?: ReactNode;
}

export function DatabaseAccessPopover({
    accessLevel = "DatabaseAdmin",
    customAccessRequiredMessage,
    children,
    conditions,
    ...props
}: DatabaseAdminAccessPopoverProps) {
    const canHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation);
    const hasRequiredAccess = canHandleOperation(accessLevel);
    const databaseAccessCondition: Condition = {
        isActive: !hasRequiredAccess,
        message: customAccessRequiredMessage || getDatabaseAccessRequiredMessage(accessLevel),
    };
    const normalizedAdditionalConditions: Condition[] = conditions
        ? Array.isArray(conditions)
            ? conditions
            : [conditions]
        : [];

    const allConditions: Condition[] = [databaseAccessCondition, ...normalizedAdditionalConditions];

    return (
        <ConditionalPopover {...props} conditions={allConditions}>
            {children}
        </ConditionalPopover>
    );
}

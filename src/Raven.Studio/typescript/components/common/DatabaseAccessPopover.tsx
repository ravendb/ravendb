import React from "react";
import { ConditionalPopover, ConditionalPopoverProps } from "components/common/ConditionalPopover";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { getDatabaseAccessRequiredMessage } from "components/utils/accessUtils";

export interface DatabaseAccessPopoverProps extends Omit<ConditionalPopoverProps, "conditions"> {
    accessRequired: databaseAccessLevel;
    conditions?: ConditionalPopoverProps["conditions"];
}

export function DatabaseAccessPopover({
    conditions = [],
    children,
    accessRequired = "DatabaseAdmin",
    ...rest
}: DatabaseAccessPopoverProps) {
    const canHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation)(accessRequired);

    const additionalConditions = Array.isArray(conditions) ? conditions : [conditions];
    return (
        <ConditionalPopover
            {...rest}
            conditions={[
                {
                    isActive: !canHandleOperation,
                    message: getDatabaseAccessRequiredMessage(accessRequired),
                },
                ...additionalConditions,
            ]}
        >
            {children}
        </ConditionalPopover>
    );
}

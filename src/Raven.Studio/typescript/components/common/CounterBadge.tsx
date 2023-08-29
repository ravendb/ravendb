import React, { ReactNode } from "react";
import classNames from "classnames";
import { Badge, UncontrolledPopover } from "reactstrap";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { uniqueId } from "lodash";

interface CounterBadgeProps {
    count: number;
    limit?: number;
    limitMessage?: ReactNode | ReactNode[];
    className?: string;
    hideNotReached?: boolean;
}

export function CounterBadge(props: CounterBadgeProps) {
    const { count, limit, limitMessage, className, hideNotReached } = props;

    const limitReachStatus = getLicenseLimitReachStatus(count, limit);
    const badgeId = "counterBadge" + uniqueId();

    return (
        <>
            {limitReachStatus !== "notReached" ? (
                <>
                    <Badge
                        pill
                        color={limitReachStatus === "limitReached" ? "danger" : "warning"}
                        className={classNames("text-dark", className)}
                        id={badgeId}
                    >
                        {count} / {limit}
                    </Badge>
                    <UncontrolledPopover target={badgeId} trigger="hover" placement="top" className="bs5">
                        {limitMessage ? (
                            limitMessage
                        ) : (
                            <div className="p-2">
                                License limit: <strong>{limit}</strong>
                            </div>
                        )}
                    </UncontrolledPopover>
                </>
            ) : (
                <>
                    {!hideNotReached && (
                        <Badge pill className={className}>
                            {count}
                        </Badge>
                    )}
                </>
            )}
        </>
    );
}

import { ReactNode } from "react";
import classNames from "classnames";
import Badge from "react-bootstrap/Badge";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import PopoverWithHoverWrapper from "./PopoverWithHoverWrapper";

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

    return (
        <>
            {limitReachStatus !== "notReached" ? (
                <PopoverWithHoverWrapper
                    message={
                        limitMessage ? (
                            limitMessage
                        ) : (
                            <span>
                                License limit: <strong>{limit}</strong>
                            </span>
                        )
                    }
                >
                    <Badge
                        pill
                        bg={limitReachStatus === "limitReached" ? "danger" : "warning"}
                        className={classNames("counter-badge text-dark", className)}
                    >
                        {count} / {limit}
                    </Badge>
                </PopoverWithHoverWrapper>
            ) : (
                <>
                    {!hideNotReached && (
                        <Badge pill className={className} bg="secondary">
                            {count}
                        </Badge>
                    )}
                </>
            )}
        </>
    );
}

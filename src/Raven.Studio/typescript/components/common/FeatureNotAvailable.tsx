import React, { ReactNode } from "react";
import Badge from "react-bootstrap/Badge";
import { EmptySet } from "./EmptySet";

interface FeatureNotAvailableProps {
    badgeText?: string;
    children: ReactNode | ReactNode[];
}

export default function FeatureNotAvailable(props: FeatureNotAvailableProps) {
    const { badgeText, children } = props;
    return (
        <div>
            <EmptySet icon="disabled" color="warning">
                <div className="vstack gap-3">
                    <span>
                        <Badge pill bg="faded-warning">
                            {badgeText ?? "Feature not available"}
                        </Badge>
                    </span>
                    {children}
                </div>
            </EmptySet>
        </div>
    );
}

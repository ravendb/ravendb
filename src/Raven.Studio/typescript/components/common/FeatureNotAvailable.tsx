import React, { ReactNode } from "react";
import { Badge } from "reactstrap";
import { EmptySet } from "./EmptySet";

interface FeatureNotAvailableProps {
    children: ReactNode | ReactNode[];
}

export default function FeatureNotAvailable({ children }: FeatureNotAvailableProps) {
    return (
        <div>
            <EmptySet icon="disabled" color="warning">
                <div className="vstack gap-3">
                    <span>
                        <Badge pill color="faded-warning">
                            Feature not available
                        </Badge>
                    </span>
                    {children}
                </div>
            </EmptySet>
        </div>
    );
}

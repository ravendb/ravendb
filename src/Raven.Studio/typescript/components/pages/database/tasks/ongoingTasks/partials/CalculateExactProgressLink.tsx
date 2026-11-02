import React from "react";
import Spinner from "react-bootstrap/Spinner";
import { Icon } from "components/common/Icon";
import { withPreventDefault } from "components/utils/common";

interface CalculateExactProgressLinkProps {
    isCalculating: boolean;
    calculate: () => void;
}

export function CalculateExactProgressLink({ isCalculating, calculate }: CalculateExactProgressLinkProps) {
    return (
        <div className="text-center">
            <a href="#" onClick={withPreventDefault(calculate)}>
                {isCalculating ? <Spinner size="sm" className="me-1" /> : <Icon icon="refresh" margin="me-1" />}
                calculate exact counts
            </a>
        </div>
    );
}

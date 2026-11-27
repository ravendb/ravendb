import React from "react";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

interface CalculateExactProgressButtonProps {
    isCalculating: boolean;
    calculate: () => void;
}

export function CalculateExactProgressButton({ isCalculating, calculate }: CalculateExactProgressButtonProps) {
    return (
        <ButtonWithSpinner
            variant="link"
            size="xs"
            className="p-0 mx-auto"
            icon="refresh"
            isSpinning={isCalculating}
            onClick={calculate}
        >
            calculate exact counts
        </ButtonWithSpinner>
    );
}

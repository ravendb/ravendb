import { calculateThreshold } from "components/utils/licenseLimitsUtils";
import React, { ReactNode, useState } from "react";

interface LicenseLimitThresholdProps {
    children: ReactNode | ReactNode[];
    count: number;
    limit: number;
}

export function LicenseLimitThreshold(props: LicenseLimitThresholdProps): JSX.Element {
    const { children, count, limit } = props;
    const threshold = calculateThreshold(limit);
    const [showAlert, setShowAlert] = useState<boolean>(count >= threshold);

    return <>{showAlert && children}</>;
}

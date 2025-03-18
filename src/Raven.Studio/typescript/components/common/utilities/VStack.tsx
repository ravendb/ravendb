import React, { PropsWithChildren } from "react";
import classNames from "classnames";
import { Gap, getGapClasses } from "components/common/utilities/stackCommon";

export interface VStackProps {
    className?: string;
    gap?: Gap;
}

export function VStack({ className, children, gap }: PropsWithChildren<VStackProps>) {
    const gapClasses = getGapClasses(gap);

    return <div className={classNames("vstack", className, ...gapClasses)}>{children}</div>;
}

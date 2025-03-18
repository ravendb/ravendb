import React, { PropsWithChildren } from "react";
import classNames from "classnames";
import { Gap, getGapClasses } from "components/common/utilities/stackCommon";

export interface HStackProps {
    className?: string;
    gap?: Gap;
}

export function HStack({ className, children, gap }: PropsWithChildren<HStackProps>) {
    const gapClasses = getGapClasses(gap);

    return <div className={classNames("hstack flex-wrap", className, ...gapClasses)}>{children}</div>;
}

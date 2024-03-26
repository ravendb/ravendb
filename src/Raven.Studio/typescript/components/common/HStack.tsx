import React, { PropsWithChildren } from "react";
import classNames from "classnames";
import { ClassNameProps } from "components/models/common";

export function HStack({ className, children }: PropsWithChildren<ClassNameProps>) {
    return <div className={classNames("hstack", className)}>{children}</div>;
}

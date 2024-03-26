import React, { PropsWithChildren } from "react";
import classNames from "classnames";
import { ClassNameProps } from "components/models/common";

export function VStack({ className, children }: PropsWithChildren<ClassNameProps>) {
    return <div className={classNames("vstack", className)}>{children}</div>;
}

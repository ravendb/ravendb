import classNames from "classnames";
import { LabelHTMLAttributes, ReactNode } from "react";

interface LabelProps extends LabelHTMLAttributes<HTMLLabelElement> {
    children: ReactNode;
}

export default function Label({ children, className, ...props }: LabelProps) {
    return (
        <label className={classNames("mb-1", className)} {...props}>
            {children}
        </label>
    );
}

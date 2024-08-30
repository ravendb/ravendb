import classNames from "classnames";
import React, { ReactNode } from "react";

interface LazyLoadProps {
    children?: ReactNode | ReactNode[];
    active: boolean;
    className?: string;
}

export function LazyLoad(props: LazyLoadProps): JSX.Element {
    const { children, active, className } = props;

    if (active) {
        return (
            <div className={classNames("lazy-load", className)} data-testid="loader">
                {children}
            </div>
        );
    }

    return <React.Fragment>{children}</React.Fragment>;
}

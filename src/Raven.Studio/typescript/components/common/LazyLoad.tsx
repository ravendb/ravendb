import React, { ReactNode } from "react";

import "./LazyLoad.scss";

interface LazyLoadProps {
    children?: ReactNode | ReactNode[];
    active?: boolean;
}

export function LazyLoad(props: LazyLoadProps): JSX.Element {
    const { children, active } = props;

    if (active) {
        return <div className="lazy-load">{children}</div>;
    }

    return <React.Fragment>{children}</React.Fragment>;
}

LazyLoad.defaultProps = {
    active: true,
};

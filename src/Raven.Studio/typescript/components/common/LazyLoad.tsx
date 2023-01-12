import React, { ReactNode } from "react";

interface LazyLoadProps {
    children?: ReactNode | ReactNode[];
    active?: boolean;
}

export function LazyLoad(props: LazyLoadProps) {
    const { children, active } = props;

    return <>{active ? <div className="lazy-load">{children}</div> : <>{children}</>}</>;
}

LazyLoad.defaultProps = {
    active: true,
};

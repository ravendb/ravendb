import React, { CSSProperties } from "react";
import classNames from "classnames";
import "./TextShimmer.scss";

export interface TextShimmerProps extends React.HTMLAttributes<HTMLElement> {
    duration?: number;
    spread?: number;
    children: string;
}

export function TextShimmer({ className, duration = 2, spread = 25, children, ...props }: TextShimmerProps) {
    const dynamicSpread = Math.min(Math.max(spread, 5), 45);

    const style: CSSProperties = {
        backgroundImage: `linear-gradient(to right, var(--text-muted) ${50 - dynamicSpread}%, var(--text-emphasis) 50%, var(--text-muted) ${50 + dynamicSpread}%)`,
        animationDuration: `${duration}s`,
        ...props.style,
    };

    return (
        <span className={classNames("text-shimmer", className)} style={style} {...props}>
            {children}
        </span>
    );
}

import "./DotList.scss";
import { ReactNode } from "react";
import classNames from "classnames";
import { GapNumber } from "../utilities/stackCommon";
import { ThemeColor } from "components/models/common";

type Color = ThemeColor | `faded-${ThemeColor}`;

interface DotListProps {
    items: ReactNode[];
    className?: string;
    gap?: GapNumber;
    dotColor?: Color;
    lineColor?: Color;
}

export function DotList(props: DotListProps) {
    const { items, className, gap = 1, dotColor = "primary", lineColor = "secondary" } = props;

    return (
        <div className={classNames("dot-list", className)}>
            {items.map((item, idx) => (
                <div key={idx} className="dot-list-item">
                    <div className="dot-list-item-line-container">
                        <div className={classNames("dot-list-item-dot", `bg-${dotColor}`)}></div>
                        <div className={classNames("dot-list-item-line", `bg-${lineColor}`)}></div>
                    </div>
                    <div className={classNames("dot-list-item-content", { [`pb-${gap}`]: idx < items.length - 1 })}>
                        {item}
                    </div>
                </div>
            ))}
        </div>
    );
}

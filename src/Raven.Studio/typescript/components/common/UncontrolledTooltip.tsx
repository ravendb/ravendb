import React, { useEffect } from "react";
import ReactDOM from "react-dom";
import { createRoot } from "react-dom/client";

interface UncontrolledTooltipProps extends Exclude<TooltipOptions, "title" | "html"> {
    target: string;
    children: JSX.Element;
}

export function UncontrolledTooltip(props: UncontrolledTooltipProps) {
    const { target, children, ...rest } = props;

    useEffect(() => {
        const container = document.createElement("div");
        const root = createRoot(container);

        root.render(
            <div
                className="tooltip-container"
                ref={() => {
                    $("#" + target).tooltip({
                        title: container.querySelector(".tooltip-container").innerHTML,
                        html: true,
                        ...rest,
                    });
                }}
            >
                {children}
            </div>
        );

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [target]);

    return null as JSX.Element;
}

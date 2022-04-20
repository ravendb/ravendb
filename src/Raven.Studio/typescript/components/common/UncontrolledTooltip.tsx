import React from "react";
import { useEffect } from "react";
import ReactDOM from "react-dom";

interface UncontrolledTooltipProps extends Exclude<TooltipOptions, "title" | "html"> {
    target: string;
    children: JSX.Element;
}

export function UncontrolledTooltip(props: UncontrolledTooltipProps) {
    const { target, children, ...rest } = props;

    useEffect(() => {
        const container = document.createElement("div");
        ReactDOM.render(children, container, () => {
            $("#" + target).tooltip({
                title: container.innerHTML,
                html: true,
                ...rest,
            });
        });

        return () => {
            ReactDOM.unmountComponentAtNode(container);
        };
    }, [target]);

    return null as JSX.Element;
}

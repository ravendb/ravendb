import { useEffect, useRef } from "react";
import ReactDOM from "react-dom";
import popoverUtils from "common/popoverUtils";

interface PopoverWithHoverProps extends PopoverUtilsOptions {
    target: string;
    children: JSX.Element;
}

export function PopoverWithHover(props: PopoverWithHoverProps) {
    const { target, children, ...rest } = props;

    const textRef = useRef<string>();

    useEffect(() => {
        const container = document.createElement("div");
        ReactDOM.render(children, container, () => {
            textRef.current = container.innerHTML;
        });

        return () => {
            ReactDOM.unmountComponentAtNode(container);
        };
    }, [children]);

    useEffect(() => {
        const $target = $("#" + target);
        popoverUtils.longWithHover($target, {
            content: () => textRef.current,
            html: true,
            sanitize: false,
            ...rest,
        } as any);
    }, [target]);

    return null as JSX.Element;
}

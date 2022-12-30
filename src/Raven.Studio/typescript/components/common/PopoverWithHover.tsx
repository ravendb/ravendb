import { useEffect, useRef } from "react";
import ReactDOM from "react-dom";
import popoverUtils from "common/popoverUtils";
import { createRoot, Root } from "react-dom/client";

interface PopoverWithHoverProps extends PopoverUtilsOptions {
    target: string;
    children: JSX.Element | JSX.Element[];
}

export function PopoverWithHover(props: PopoverWithHoverProps) {
    const { target, children, ...rest } = props;

    const textRef = useRef<HTMLDivElement>();
    const rootRef = useRef<Root>();

    useEffect(() => {
        textRef.current = document.createElement("div");
        rootRef.current = createRoot(textRef.current);
    }, []);

    useEffect(() => {
        rootRef.current.render(children);
    }, [children]);

    useEffect(() => {
        const $target = $("#" + target);
        popoverUtils.longWithHover($target, {
            content: () => textRef.current,
            html: true,
            sanitize: false,
            ...rest,
        } as any);
        // eslint-disable-next-line
    }, [target]);

    return null as JSX.Element;
}

import { useEffect, useRef } from "react";
import ReactDOM from "react-dom";
import popoverUtils from "common/popoverUtils";

interface PopoverWithHoverProps extends PopoverUtilsOptions {
    target: string;
    children: JSX.Element | JSX.Element[];
}

export function PopoverWithHover(props: PopoverWithHoverProps) {
    const { target, children, ...rest } = props;

    const textRef = useRef<HTMLDivElement>();

    useEffect(() => {
        textRef.current = document.createElement("div");
    }, []);

    useEffect(() => {
        ReactDOM.render(children, textRef.current);
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

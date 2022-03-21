import { MouseEvent, MouseEventHandler } from "react";

export function withPreventDefault(action: Function): MouseEventHandler<HTMLElement> {
    return (e: MouseEvent<HTMLElement>) => {
        e.preventDefault();
        action();
    }
}

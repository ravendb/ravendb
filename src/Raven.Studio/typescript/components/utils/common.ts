import { MouseEvent, MouseEventHandler } from "react";

export function withPreventDefault(action: Function): MouseEventHandler<HTMLElement> {
    return (e: MouseEvent<HTMLElement>) => {
        e.preventDefault();
        action();
    };
}

export function databaseLocationComparator(lhs: databaseLocationSpecifier, rhs: databaseLocationSpecifier) {
    return lhs.nodeTag === rhs.nodeTag && lhs.shardNumber === rhs.shardNumber;
}

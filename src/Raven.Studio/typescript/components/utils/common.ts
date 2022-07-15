import { MouseEvent, MouseEventHandler } from "react";
import { Story, StoryFn } from "@storybook/react";

export function withPreventDefault(action: Function): MouseEventHandler<HTMLElement> {
    return (e: MouseEvent<HTMLElement>) => {
        e.preventDefault();
        action();
    };
}

export async function delay(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export function databaseLocationComparator(lhs: databaseLocationSpecifier, rhs: databaseLocationSpecifier) {
    return lhs.nodeTag === rhs.nodeTag && lhs.shardNumber === rhs.shardNumber;
}

export function boundCopy<TArgs>(story: StoryFn<TArgs>, args?: TArgs): Story<TArgs> {
    const copy = story.bind({});
    copy.args = args;
    return copy;
}

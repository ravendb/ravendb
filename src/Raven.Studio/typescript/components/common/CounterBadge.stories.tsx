import { ComponentMeta } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { CounterBadge } from "./CounterBadge";

export default {
    title: "Bits/CounterBadge",
    component: CounterBadge,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof CounterBadge>;

export function CounterBadges() {
    return (
        <div className="vstack gap-2">
            <div>
                <code>Count = 3</code>
                <CounterBadge count={3} className="ms-1" />
            </div>
            <div>
                <code>Count = 3</code> <code>Limit = -1</code> (Unlimited) =
                <CounterBadge count={3} limit={-1} className="ms-1" />
            </div>
            <div>
                Limit <code>notReached</code> <CounterBadge count={3} limit={6} className="ms-1" />
            </div>
            <div>
                <code>closeToLimit</code>
                <CounterBadge count={5} limit={6} className="ms-1" />
            </div>
            <div>
                <code>limitReached</code>
                <CounterBadge count={6} limit={6} className="ms-1" />
            </div>
        </div>
    );
}

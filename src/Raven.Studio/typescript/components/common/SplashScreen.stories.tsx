import { ComponentMeta } from "@storybook/react";
import { SplashScreen } from "./SplashScreen";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/SplashScreen",
    decorators: [withStorybookContexts, withBootstrap5],
    component: SplashScreen,
} as ComponentMeta<typeof SplashScreen>;

export function Loading() {
    return <SplashScreen />;
}

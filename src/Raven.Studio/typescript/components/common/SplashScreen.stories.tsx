// This component is only displayed in the storybook. To modify the actual splash screen, make changes in wwwroot/index.html

import { Meta } from "@storybook/react";
import { SplashScreen } from "./SplashScreen";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/SplashScreen",
    decorators: [withStorybookContexts, withBootstrap5],
    component: SplashScreen,
} satisfies Meta<typeof SplashScreen>;

export function Loading() {
    return <SplashScreen />;
}

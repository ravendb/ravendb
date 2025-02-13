// This component is only displayed in the storybook. To modify the actual splash screen, make changes in wwwroot/index.html

import { Meta } from "@storybook/react";
import { SplashScreen } from "./SplashScreen";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/SplashScreen",
    decorators: [withStorybookContexts, withBootstrap5],
    component: SplashScreen,
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=14-499",
        },
    },
} satisfies Meta<typeof SplashScreen>;

export function Loading() {
    return <SplashScreen />;
}

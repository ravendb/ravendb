import { resetAllMocks } from "@storybook/test";
import { useState } from "react";
import { createStoreConfiguration } from "../typescript/components/store";
import { setEffectiveTestStore } from "../typescript/components/storeCompat";
import { Provider } from "react-redux";
import React from "react";

export const StoreDecorator = (Story, context) => {
    useTheme(context.globals.theme);
    resetAllMocks();

    const [store] = useState(() => {
        const storeConfiguration = createStoreConfiguration();
        setEffectiveTestStore(storeConfiguration);
        return storeConfiguration;
    });

    return (
        <Provider store={store}>
            <div className="h-100">
                <Story />
            </div>
        </Provider>
    );
};

const stylesheetPrefix = "styles/";

export type Theme = "dark" | "light" | "blue" | "classic";

const themeToStylesheet: Record<Theme, string> = {
    dark: "styles.css",
    light: "styles-light.css",
    blue: "styles-blue.css",
    classic: "styles-classic.css",
};

function useTheme(theme: Theme) {
    const fileName = themeToStylesheet[theme];

    // remove all style links
    Object.values(themeToStylesheet).forEach((fileName) => {
        document.querySelector(`link[href="styles/bs5-${fileName}"]`)?.remove();
        document.querySelector(`link[href="styles/${fileName}"]`)?.remove();
    });

    // add new style links
    const bs5ThemeLink = document.createElement("link");
    bs5ThemeLink.rel = "stylesheet";
    bs5ThemeLink.href = stylesheetPrefix + "bs5-" + fileName;
    document.head.insertBefore(bs5ThemeLink, document.head.firstChild);

    const themeLink = document.createElement("link");
    themeLink.rel = "stylesheet";
    themeLink.href = stylesheetPrefix + fileName;
    document.head.insertBefore(themeLink, document.head.firstChild);
}

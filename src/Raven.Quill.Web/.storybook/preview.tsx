import "@/index.css";
import type { Preview } from "@storybook/react-vite";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { withProviders } from "./decorators";
import { mswLoader } from "./mock-service-worker";
import { resetAllStores } from "./zustand";

const preview: Preview = {
    decorators: [withProviders],
    loaders: [mswLoader],
    beforeEach: resetAllStores,
    initialGlobals: {
        theme: "light",
    },
    globalTypes: {
        theme: {
            description: "App theme",
            toolbar: {
                title: "Theme",
                icon: "paintbrush",
                items: [
                    { value: "light", title: "Light", icon: "sun" },
                    { value: "dark", title: "Dark", icon: "moon" },
                ],
                dynamicTitle: true,
            },
        },
    },
    parameters: {
        layout: "fullscreen",
        controls: {
            matchers: {
                color: /(background|color)$/i,
                date: /Date$/i,
            },
        },
        msw: {
            handlers: defaultApiMocks,
        },
    },
};

export default preview;

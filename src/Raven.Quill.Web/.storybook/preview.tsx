import "@/index.css";
import type { Preview } from "@storybook/react-vite";
import { initialize, mswLoader } from "msw-storybook-addon";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { withProviders } from "./decorators";

// MSW serves story API requests from a service worker, so the app's networking code
// runs unchanged. Non-API traffic (Storybook internals, HMR, fonts) passes through
// silently; an unmocked server endpoint is a story bug, so make it loud.
initialize({
    onUnhandledRequest: (request, print) => {
        if (new URL(request.url).pathname.startsWith("/api/")) {
            print.warning();
        }
    },
});

const preview: Preview = {
    decorators: [withProviders],
    loaders: [mswLoader],
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

import type { Loader } from "@storybook/react-vite";
import { initialize, mswLoader as addonMswLoader } from "msw-storybook-addon";

function startWorker() {
    initialize({
        onUnhandledRequest: (request, print) => {
            if (new URL(request.url).pathname.startsWith("/api/")) {
                print.warning();
            }
        },
    });

    // The addon's loader is its only way to wait until the worker is ready.
    return addonMswLoader({ parameters: {} });
}

// Kept out of preview.tsx, which runs again on every mock edit and would restart the worker.
// Every story waits on this one start and the addon never retries, so a single failed
// registration would break all stories until a reload.
const workerReady = startWorker()
    .catch(() => startWorker())
    .catch((error: unknown) => {
        const details = error instanceof Error ? error.message : String(error);
        throw new Error(
            "The API mocks cannot run because the service worker failed to register. Some embedded browsers cannot register service workers, so try Storybook in Chrome. The original error is below." +
                `\n\n${details}`,
            { cause: error },
        );
    });

// Every loader reports the failure, so it is not an unhandled rejection.
workerReady.catch(() => {});

export const mswLoader: Loader = async (context) => {
    await workerReady;
    return addonMswLoader(context);
};

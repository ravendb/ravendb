import { setupWorker } from "msw/browser";
import { mswLoader as addonMswLoader } from "msw-storybook-addon/csf3";

async function startWorker() {
    const worker = setupWorker();
    await worker.start({
        onUnhandledRequest: (request, print) => {
            if (new URL(request.url).pathname.startsWith("/api/")) {
                print.warning();
            }
        },
    });

    return worker;
}

// Every story waits on this one start and the addon never retries, so a single failed
// registration would break all stories until a reload.
function startWorkerWithRetry() {
    return startWorker()
        .catch(() => startWorker())
        .catch((error: unknown) => {
            const details = error instanceof Error ? error.message : String(error);
            throw new Error(
                "The API mocks cannot run because the service worker failed to register. Some embedded browsers cannot register service workers, so try Storybook in Chrome. The original error is below." +
                    `\n\n${details}`,
                { cause: error },
            );
        });
}

export const mswLoader = addonMswLoader(startWorkerWithRetry);

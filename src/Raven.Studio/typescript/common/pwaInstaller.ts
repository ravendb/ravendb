/// <reference path="../../typings/tsd.d.ts" />

/**
 * Listens for browser events allowing progressive web apps (PWAs) to be installed.
 * For more info, see https://developers.google.com/web/fundamentals/app-install-banners
 * */
class pwaInstaller {

    static readonly instance = new pwaInstaller();

    private deferredInstallPrompt: BeforeInstallPromptEvent | null = null;

    constructor() {
        // Browsers will trigger this event when it deems appropriate (e.g. the user has used our app often).
        window.addEventListener('beforeinstallprompt', (e: BeforeInstallPromptEvent) => {
            // Prevent Chrome 67 and earlier from automatically showing the prompt
            e.preventDefault();
            // Stash the event so it can be triggered later.
            this.deferredInstallPrompt = e;
        });
    }

    get canInstall(): boolean {
        return !!this.deferredInstallPrompt;
    }

    install(): Promise<InstallPromptResult> | null {
        if (this.canInstall && !!this.deferredInstallPrompt) {
            // Show the prompt
            this.deferredInstallPrompt.prompt();

            // Wait for the user to respond to the prompt.
            return this.deferredInstallPrompt.userChoice;
        }

        // We can't install, so just return null.
        return null;
    }
}

/**
 * The BeforeInstallPromptEvent is fired at the Window.onbeforeinstallprompt handler
 * before a user is prompted to "install" a web site to a home screen or desktop.
 * https://developers.google.com/web/fundamentals/app-install-banners/
 */
interface BeforeInstallPromptEvent extends Event {

    /**
     * Returns an array of DOMString items containing the platforms on which the event was dispatched.
     * This is provided for user agents that want to present a choice of versions to the user such as,
     * for example, "web" or "play" which would allow the user to chose between a web version or
     * an Android version.
     */
    readonly platforms: string[];

    /**
     * Returns a Promise that resolves to a DOMString containing either "accepted" or "dismissed".
     */
    readonly userChoice: Promise<InstallPromptResult>;

    /**
     * Allows a developer to show the install prompt at a time of their own choosing.
     * This method returns a Promise.
     */
    prompt(): Promise<void>;
}

interface InstallPromptResult {
    outcome: 'accepted' | 'dismissed';
    platform: string;
}

export = pwaInstaller;

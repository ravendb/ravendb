/// <reference path="../../typings/tsd.d.ts" />

/**
 * Service that aids in performing installation for progressive web apps (PWAs).
 * For more info, see https://developers.google.com/web/fundamentals/app-install-banners
 * */
class pwaInstaller {
    
    private deferredInstallPrompt: BeforeInstallPromptEvent | null = null;

    constructor() {
        // This install prompt sent to us by the browser at app initialization time (see main.ts)
        // It will be sent to us if the browser has determined installation is allowed (e.g. we've used the Studio for awhile).
        const installPrompt = (window as any).ravenStudioInstallPrompt;
        if (installPrompt) {
            this.deferredInstallPrompt = installPrompt;
        }
    }

    get canInstallApp(): boolean {
        return !!this.deferredInstallPrompt;
    }

    promptInstallApp(): Promise<InstallPromptResult> | null {
        if (!!this.deferredInstallPrompt) {
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

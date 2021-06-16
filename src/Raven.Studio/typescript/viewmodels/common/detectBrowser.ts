import studioSettings = require("common/settings/studioSettings");

class detectBrowser {

    showBrowserAlert = ko.observable<boolean>(false);
    dontShowBrowserAlertAgain = ko.observable<boolean>(false);
    
    constructor(private infoPersistedInSettings: boolean) {
        const isBrowserSupported = detectBrowser.isBrowserSupported();

        if (this.infoPersistedInSettings) {
            studioSettings.default.globalSettings()
                .done(settings => {
                    if (settings.dontShowAgain.shouldShow("UnsupportedBrowser")) {
                        this.showBrowserAlert(!isBrowserSupported);
                    }
                });
        } else {
            this.showBrowserAlert(!isBrowserSupported);
        }
    }

    static isBrowserSupported(): boolean {
        const isChrome = /Chrome/.test(navigator.userAgent) && /Google Inc/.test(navigator.vendor);
        const isFirefox = navigator.userAgent.toLowerCase().indexOf('firefox') > -1;

        return isChrome || isFirefox;
    }

    continueWithCurrentBrowser(): void {
        if (this.infoPersistedInSettings && this.dontShowBrowserAlertAgain()) {
            studioSettings.default.globalSettings()
                .done(settings => settings.dontShowAgain.ignore("UnsupportedBrowser"));
        }
        
        this.showBrowserAlert(false);
    }
}

export = detectBrowser;

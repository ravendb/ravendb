require("google.analytics");

class eventsCollector {
    static UACode = "UA-82335022-3";

    static default = new eventsCollector();

    // used for caching events fired before analytics initialization
    // if user don't agree on usage stats tracking we discard this data
    preInitializationQueue: Function[] = [];

    version: string;
    build: number;
    env: string;
    fullVersion: string;
    enabled = false;
    initialized = false;

    initialize(version: string, build: number, env: string, fullVersion: string, enabled: boolean) {
        this.version = version;
        this.build = build;
        this.env = env;
        this.fullVersion = fullVersion;
        this.enabled = enabled && eventsCollector.gaDefined();
        this.createTracker();

        this.initialized = true;

        this.processQueue();
    }

    static gaDefined() {
        return typeof (ga) !== 'undefined';
    }

    createTracker() {
        if (eventsCollector.gaDefined()) {
            ga('create', eventsCollector.UACode, 'auto');
            ga('set', 'dimension1', this.version);
            ga('set', 'dimension2', this.build);
            ga('set', 'dimension3', this.env);
            ga('set', 'dimension4', this.fullVersion);
        }
    }

    processQueue() {
        if (this.enabled) {
            this.preInitializationQueue.forEach(action => {
                action(ga);
            });
        }

        this.preInitializationQueue = [];
    }

    reportViewModel(view: any) {
        const viewName = view.__moduleId__;
        this.internalLog((ga) => {
            ga('set', 'location', `http://raven.studio/${viewName}${document.location.search}`);
            ga('send', 'pageview');
        });
    }

    reportEvent(category: string, action: string, label: string = null) {
        this.internalLog((ga) => {
            ga('send', 'event', category, action, label);
        });
    }

    private internalLog(action: (ga: UniversalAnalytics.ga) => void) {
        if (!this.initialized) {
            this.preInitializationQueue.push(action);
            return;
        }
        if (!this.enabled) {
            return;
        }
        action(ga);
    }

}

export = eventsCollector;


class eventsCollector {
    static UACode = "UA-82090983-1"; //TODO: change me!

    static default = new eventsCollector();

    // used for caching events fired before analytics initialization
    // if user don't agree on usage stats tracking we discard this data
    preInitializationQueue: Function[] = [];

    version: string;
    build: number;
    enabled = false;
    initialized = false;

    initialize(version: string, build: number, enabled: boolean) {
        this.version = version;
        this.build = build;
        this.enabled = enabled;
        this.createTracker();

        this.initialized = true;

        this.processQueue();
    }

    createTracker() {
        ga('create', eventsCollector.UACode, 'auto');
        ga('set', 'dimension1', this.version);
        ga('set', 'dimension2', this.build);
    }

    processQueue() {
        if (this.enabled) {
            this.preInitializationQueue.forEach(action => {
                action();
            });
        }
        
        this.preInitializationQueue = [];
    }

    reportViewModel(view: any) {
        this.internalLog((ga) => {
            ga('send', 'pageview', view.__moduleId__);
        });
    }

    reportEvent(category: string, action: string, label: string = null) {
        this.internalLog((ga) => {
            ga('send', 'event', category, action, label);
        });
    }

    private internalLog(action: (ga: UniversalAnalytics.ga) => void) {
        if (!this.initialized) {
            this.preInitializationQueue.push(ga);
            return;
        }
        if (!this.enabled) {
            return;
        }
        action(ga);
    }

}

export = eventsCollector;

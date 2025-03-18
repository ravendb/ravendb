import router = require("plugins/router");

type gtagFn = (key: string, value: any, params?: any) => void;

(window as any).dataLayer = (window as any).dataLayer || [];
function gtag() {
    // eslint-disable-next-line prefer-rest-params
    (window as any).dataLayer.push(arguments);
}


class eventsCollector {
    static readonly TrackingCode = "G-V0B01LBCM5";
    // controls GTag Debug mode
    static readonly DebugMode = false;

    static default = new eventsCollector();

    // used for caching events fired before analytics initialization
    // if user don't agree on usage stats tracking we discard this data
    preInitializationQueue: Array<(gtag: gtagFn) => void> = [];

    private version: string;
    private build: number;
    private environment: Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
    private fullVersion: string;
    private enabled = false;
    private initialized = false;
    private licenseStatusProvider: () => LicenseStatus;
    private supportInfoProvider: () => Raven.Server.Commercial.LicenseSupportInfo;


    constructor() {
        _.bindAll(this, "reportEvent");
    }

    initialize(version: string,
               build: number,
               environment: Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment,
               fullVersion: string,
               licenseStatusProvider: () => LicenseStatus,
               supportInfoProvider: () => Raven.Server.Commercial.LicenseSupportInfo,
               enabled: boolean) {
        this.version = version;
        this.build = build;
        this.environment = environment;
        this.fullVersion = fullVersion;
        this.licenseStatusProvider = licenseStatusProvider;
        this.supportInfoProvider = supportInfoProvider;
        this.initializeTracker();
        this.setEnabled(enabled);

        this.initialized = true;
    }

    private initializeTracker() {
        const gtagProxy = gtag as any;
        gtagProxy("js", new Date());
        gtagProxy("config", eventsCollector.TrackingCode, {
            send_page_view: false,
            debug_mode: eventsCollector.DebugMode,
        });
        gtagProxy("set", "user_properties", this.getUserProperties());
        
        this.preInitializationQueue.push(() => {
            this.initGtag();
        });
    }

    private initGtag() {
        const url  = "https://www.googletagmanager.com/gtag/js?id=" + eventsCollector.TrackingCode;
        const gtmScript = document.createElement("script");
        gtmScript.setAttribute("src",url);
        document.head.appendChild(gtmScript);
    }

    private getUserProperties() {
        const licenseStatus = this.licenseStatusProvider();
        const supportInfo = this.supportInfoProvider();
        return {
            version: this.version,
            full_version: this.fullVersion,
            environment: this.environment,
            build: this.build,
            license_type: licenseStatus?.Type ?? "N/A",
            support_type: supportInfo?.Status ?? "N/A",
            is_cloud: licenseStatus ? licenseStatus.IsCloud : "N/A",
            is_isv: licenseStatus ? licenseStatus.IsIsv : "N/A",
        }
    }

    setEnabled(enabled: boolean) {
        this.enabled = enabled;
        
        if (enabled) {
            this.flushPreInitializationQueue();
        }
    }

    private flushPreInitializationQueue() {
            this.preInitializationQueue.forEach(action => {
            action(gtag);
            });

        this.preInitializationQueue = [];
    }

    reportViewModel() {
        const instr = router.activeInstruction();
        const viewName = instr?.fragment;
        if (!viewName) {
            // it might be initial page load or shell initialization 
            return;
        }
        const location = `http://raven.studio/${viewName}${document.location.search}`;
        this.report((gtagProxy: gtagFn) => {
            gtagProxy('event', 'page_view', {
                page_location: location
        });
        });
    }

    reportEvent(category: string, action: string, label: string = null) {
        this.report((gtagProxy: gtagFn) => {
            gtagProxy('event', 'page_action', {
                event_category: category,
                event_action: action,
                event_label: label
        });
        });
    }

    private report(action: (gtagProxy: gtagFn) => void) {
        if (!this.initialized) {
            this.preInitializationQueue.push(action);
            return;
        }
        if (this.enabled) {
            action(gtag);
        }
    }

}

export = eventsCollector;

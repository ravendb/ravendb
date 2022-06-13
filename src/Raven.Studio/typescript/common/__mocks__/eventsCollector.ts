class eventsCollector {
    initialize(version: string, build: number, env: string, fullVersion: string, enabled: boolean) {
    }

    static gaDefined() {
        return typeof (ga) !== 'undefined';
    }

    createTracker() {
       
    }

   
    reportEvent(category: string, action: string, label: string = null) {
     
    }
}

export = eventsCollector;

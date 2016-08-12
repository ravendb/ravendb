
class liveTestDetector {
    
    static isLiveTest() {
        return location.hostname === "live-test.ravendb.net";
    }
}

export = liveTestDetector;

class sourceSynchronizationInformation {
    LastSourceFileEtag = ko.observable<string>();
    SourceServerUrl = ko.observable<string>();

    constructor(lastSynchronizedEtag: string, sourceServerUrl: string) {
        this.LastSourceFileEtag(lastSynchronizedEtag);
        this.SourceServerUrl(sourceServerUrl);
    }
}

export = sourceSynchronizationInformation;
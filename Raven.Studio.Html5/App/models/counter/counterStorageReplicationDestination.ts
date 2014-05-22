class counterStorageReplicationDestination {
    serverUrl = ko.observable<string>().extend({ required: true });
    username = ko.observable<string>().extend({ required: true });
    password = ko.observable<string>().extend({ required: true });
    domain = ko.observable<string>().extend({ required: true });
    apiKey = ko.observable<string>().extend({ required: true });
    counterStorage = ko.observable<string>().extend({ required: true });
    disabled = ko.observable<boolean>().extend({ required: true });

    name = ko.computed(() => {
        if (this.serverUrl() && this.counterStorage()) {
            return this.counterStorage() + " on " + this.serverUrl();
        } else if (this.serverUrl()) {
            return this.serverUrl();
        } else if (this.counterStorage()) {
            return this.counterStorage();
        }

        return "[empty]";
    });

    isValid = ko.computed(() => this.serverUrl() != null && this.serverUrl().length > 0);

    // data members for the ui
    isUserCredentials = ko.observable<boolean>(false);
    isApiKeyCredentials = ko.observable<boolean>(false);
    credentialsType = ko.computed(() => {
        if (this.isUserCredentials()) {
            return "user";
        } else if (this.isApiKeyCredentials()) {
            return "api-key";
        } else {
            return "none";
        }
    });


//    constructor(dto: counterServerValueDto) {
//        this.serverUrl(dto.ServerUrl);
//        this.posCount(dto.Positive);
//        this.negCount(dto.Negative);
//    }
}

export = counterStorageReplicationDestination;
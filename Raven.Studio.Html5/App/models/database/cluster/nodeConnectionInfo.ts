class nodeConnectionInfo {

    uri = ko.observable<string>();
    name = ko.observable<string>();
    username = ko.observable<string>();
    password = ko.observable<string>();
    domain = ko.observable<string>();
    apiKey = ko.observable<string>();
    state = ko.observable<string>(); // used to store owning collection name (voting, non-voting, promotable)
    status = ko.observable<string>("Loading");
    isVoting = ko.observable<boolean>();
    isLeavingCluster = ko.observable<boolean>();

    constructor(dto: nodeConnectionInfoDto) {
        this.uri(dto.Uri);
        this.name(dto.Name);
        this.username(dto.Username);
        this.password(dto.Password);
        this.domain(dto.Domain);
        this.apiKey(dto.ApiKey);
        this.isVoting(!dto.IsNoneVoter);
        if (this.username()) {
            this.isUserCredentials(true);
        } else if (this.apiKey()) {
            this.isApiKeyCredentials(true);
        }
    }

    toDto(): nodeConnectionInfoDto {
        return {
            Uri: this.uri(),
            Name: this.name(),
            Username: this.username(),
            Password: this.password(),
            Domain: this.domain(),
            ApiKey: this.apiKey(),
            IsNoneVoter: !this.isVoting()
        };
    }

    static empty(): nodeConnectionInfo {
        return new nodeConnectionInfo({
            Uri: null,
            Name: null,
            IsNoneVoter: false
        });
    }

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

    useUserCredentials() {
        this.isUserCredentials(true);
        this.isApiKeyCredentials(false);
        this.clearApiKeyCredentials();
    }

    useApiKeyCredentials() {
        this.isApiKeyCredentials(true);
        this.isUserCredentials(false);
        this.clearUserCredentials();
    }

    useNoCredentials() {
        this.isUserCredentials(false);
        this.isApiKeyCredentials(false);
        this.clearApiKeyCredentials();
        this.clearUserCredentials();
    }

    private clearApiKeyCredentials() {
        this.apiKey(null);
    }

    private clearUserCredentials() {
        this.username(null);
        this.password(null);
        this.domain(null);
    }
}

export = nodeConnectionInfo;

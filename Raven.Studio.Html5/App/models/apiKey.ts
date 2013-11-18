import apiKeyDatabase = require("models/apiKeyDatabase");
import appUrl = require("common/appUrl");

class apiKey {

    name = ko.observable<string>();
    secret = ko.observable<string>();
    fullApiKey = ko.observable<string>();
    connectionString = ko.observable<string>();
    directLink = ko.observable<string>();
    enabled = ko.observable<boolean>();
    databases = ko.observableArray<apiKeyDatabase>();

    constructor(dto: apiKeyDto, private databaseName: string) {
        this.name(dto.name);
        this.secret(dto.secret);
        this.connectionString(dto.connectionString);
        this.directLink(dto.directLink);
        this.enabled(dto.enabled);
        this.databases(dto.databases.map(d => new apiKeyDatabase(d)));
        this.fullApiKey(dto.fullApiKey);

        this.name.subscribe(newName => this.onNameOrSecretChanged(newName, this.secret()));
        this.secret.subscribe(newSecret => this.onNameOrSecretChanged(this.name(), newSecret));
    }

    static empty(databaseName: string): apiKey {
        return new apiKey({
            connectionString: "",
            databases: [],
            directLink: "",
            enabled: false,
            fullApiKey: "",
            name: "[new api key]",
            secret: ""
        }, databaseName);
    }

    enable() {
        this.enabled(true);
    }

    disable() {
        this.enabled(false);
    }

    generateSecret() {
        // The old Silverlight Studio would create a new GUID, strip out the 
        // dashes, and convert to base62.
        //
        // For the time being (is there a better way?), we're just creating a 
        // random string of alpha numeric characters.

        var minimumLength = 10;
        var maxLength = 32;
        var randomLength = Math.max(minimumLength, Math.random() * maxLength);
        var randomSecret = apiKey.randomString(randomLength, '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ');
        this.secret(randomSecret);
    }

    onNameOrSecretChanged(name: string, secret: string) {
        if (!name || !secret) {
            var errorText = "Requires name and secret";
            this.fullApiKey(errorText);
            this.connectionString(errorText);
            this.directLink(errorText);
        } else {
            var serverUrl = appUrl.forServer();
            this.fullApiKey(name + "/" + secret);
            this.connectionString("Url = " + serverUrl + "; ApiKey = " + this.fullApiKey() + "; Database = " + this.databaseName);
            this.directLink(serverUrl + "/raven/studio.html#/home?api-key=" + this.fullApiKey());
        }
    }

    private static randomString(length: number, chars: string) {
        var result = '';
        for (var i = length; i > 0; --i) result += chars[Math.round(Math.random() * (chars.length - 1))];
        return result;
    }
}

export = apiKey;
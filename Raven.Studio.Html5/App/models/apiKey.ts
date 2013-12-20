import apiKeyDatabase = require("models/apiKeyDatabase");
import appUrl = require("common/appUrl");

class apiKey {

    name = ko.observable<string>();
    secret = ko.observable<string>();
    fullApiKey: KnockoutComputed<string>;
    connectionString: KnockoutComputed<string>;
    directLink: KnockoutComputed<string>;
    enabled = ko.observable<boolean>();
    databases = ko.observableArray<apiKeyDatabase>();

    constructor(dto: apiKeyDto) {
        this.name(dto.Name);
        this.secret(dto.Secret);
        this.enabled(dto.Enabled);
        this.databases(dto.Databases.map(d => new apiKeyDatabase(d)));

        this.fullApiKey = ko.computed(() => {
            if (!this.name() || !this.secret()) {
                return "Requires name and secret";
            }

            return this.name() + "/" + this.secret();
        });

        this.connectionString = ko.computed(() => {
            if (!this.fullApiKey()) {
                return "Requires name and secret";
            }

            return "Url = " + appUrl.forServer() + "; ApiKey = " + this.fullApiKey() + "; Database = "
        });

        this.directLink = ko.computed(() => {
            if (!this.fullApiKey()) {
                return "Requires name and secret";
            }

            return appUrl.forServer() + "/raven/studio.html#/home?api-key=" + this.fullApiKey();
        });
    }

    static empty(): apiKey {
        return new apiKey({
            Databases: [],
            Enabled: false,
            Name: "[new api key]",
            Secret: ""
        });
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

    private static randomString(length: number, chars: string) {
        var result = '';
        for (var i = length; i > 0; --i) result += chars[Math.round(Math.random() * (chars.length - 1))];
        return result;
    }
}

export = apiKey;
import databaseAccess = require("models/databaseAccess");
import appUrl = require("common/appUrl");
import documentMetadata = require("models/documentMetadata");
import document = require("models/document");

class apiKey extends document {

    public name = ko.observable<string>();
    secret = ko.observable<string>();
    fullApiKey: KnockoutComputed<string>;
    connectionString: KnockoutComputed<string>;
    directLink: KnockoutComputed<string>;
    enabled = ko.observable<boolean>();
    public metadata: documentMetadata;
    databases = ko.observableArray<databaseAccess>();
    visible = ko.observable(true);

    constructor(dto: apiKeyDto) {
        super(dto);

        this.name(dto.Name);
        this.secret(dto.Secret);
        this.enabled(dto.Enabled);
        this.databases(dto.Databases.map(d => new databaseAccess(d)));
        this.metadata = new documentMetadata(dto['@metadata']);

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
            Name: "",
            Secret: ""
        });
    }

    toDto(): apiKeyDto {
        var meta = this.__metadata.toDto();
        meta['@id'] = "Raven/ApiKeys/" + this.name();
        return {
            '@metadata': meta,
            Databases: this.databases().map(db => db.toDto()),
            Enabled: this.enabled(),
            Name: this.name(),
            Secret: this.secret()
        };
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

    addEmptyApiKeyDatabase() {
        var newItem: databaseAccessDto = { TenantId: '', Admin: false, ReadOnly: false };
        this.databases.push(new databaseAccess(newItem));
    }

    removeApiKeyDatabase(database) {
        this.databases.remove(database);
    }

    setIdFromName() {
        this.__metadata.id = "Raven/ApiKeys/" + this.name();
    }

    isValid(): boolean {
        var requiredValues = [this.name(), this.secret()];
        return requiredValues.every(v => v != null && v.length > 0);
    }

    private static randomString(length: number, chars: string) {
        var result = '';
        for (var i = length; i > 0; --i) result += chars[Math.round(Math.random() * (chars.length - 1))];
        return result;
    }

    getKey(): string {
        return "Raven/ApiKeys/" + this.name();
    }

}

export = apiKey;
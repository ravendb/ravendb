/// <reference path="../../../../typings/tsd.d.ts"/>

type authenticationMethod = "none" | "apiKey" | "windows";

class migrateRavenDbDatabaseModel {
    serverUrl = ko.observable<string>();
    resourceName = ko.observable<string>();
    includeDatabaseRecord = ko.observable(true);
    includeDocuments = ko.observable(true);
    includeConflicts = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeIdentities = ko.observable(true);
    includeCompareExchange = ko.observable(true);
    includeCounters = ko.observable(true);
    includeAttachments = ko.observable(true);
    includeRevisionDocuments = ko.observable(true);
    includeLegacyAttachments = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    importRavenFs = ko.observable(false);
    showTransformScript = ko.observable<boolean>(false);
    transformScript = ko.observable<string>();

    authenticationMethod = ko.observable<authenticationMethod>("none");
    authorized = ko.observable(true);
    hasUnsecuredBasicAuthenticationOption = ko.observable(false);

    serverMajorVersion = ko.observable<Raven.Server.Smuggler.Migration.MajorVersion>("Unknown");
    buildVersion = ko.observable<number>();
    fullVersion = ko.observable<string>();
    productVersion = ko.observable<string>();
    serverUrls = ko.observableArray<string>([]);
    databaseNames = ko.observableArray<string>([]);
    fileSystemNames = ko.observableArray<string>([]);

    userName = ko.observable<string>();
    password = ko.observable<string>();
    domain = ko.observable<string>();
    apiKey = ko.observable<string>();
    enableBasicAuthenticationOverUnsecuredHttp = ko.observable<boolean>();
    skipServerCertificateValidation = ko.observable<boolean>();

    serverMajorVersionNumber: KnockoutComputed<string>;
    isRavenDb: KnockoutComputed<boolean>;
    isLegacy: KnockoutComputed<boolean>;
    isV41: KnockoutComputed<boolean>;
    hasRavenFs: KnockoutComputed<boolean>;
    ravenFsImport: KnockoutComputed<boolean>;
    resourceTypeName: KnockoutComputed<string>;
    showWindowsCredentialInputs: KnockoutComputed<boolean>;
    showApiKeyCredentialInputs: KnockoutComputed<boolean>;
    isUnsecuredBasicAuthentication: KnockoutComputed<boolean>;
    isSecuredConnection: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup;
    importDefinitionHasIncludes: KnockoutComputed<boolean>;
    versionCheckValidationGroup: KnockoutValidationGroup;

    constructor() {
        this.initObservables();
        this.initValidation();

        this.showTransformScript.subscribe(v => {
            if (v) {
                this.transformScript(
                    "this.collection = this['@metadata']['@collection'];\r\n" +
                    "// current object is available under 'this' variable\r\n" +
                    "// @change-vector, @id, @last-modified metadata fields are not available");
            } else {
                this.transformScript("");
            }
        });
    }

    toDto(): Raven.Server.Smuggler.Migration.SingleDatabaseMigrationConfiguration {
        const operateOnTypes: Array<Raven.Client.Documents.Smuggler.DatabaseItemType> = [];

        if (!this.ravenFsImport()) {
            if (this.includeDatabaseRecord()) {
                operateOnTypes.push("DatabaseRecord");
            }
            if (this.includeDocuments()) {
                operateOnTypes.push("Documents");
            }
            if (this.includeConflicts() && !this.isLegacy()) {
                operateOnTypes.push("Conflicts");
            }
            if (this.includeIndexes()) {
                operateOnTypes.push("Indexes");
            }
            if (this.includeRevisionDocuments()) {
                operateOnTypes.push("RevisionDocuments");
            }
            if (this.includeLegacyAttachments() && this.isLegacy()) {
                operateOnTypes.push("LegacyAttachments");
            }
            if (this.includeIdentities() && !this.isLegacy()) {
                operateOnTypes.push("Identities");
            }
            if (this.includeCompareExchange() && !this.isLegacy()) {
                operateOnTypes.push("CompareExchange");
            }
            if (this.includeCounters() && !this.isLegacy()) {
                operateOnTypes.push("Counters");
            }
            if (this.includeAttachments() && !this.isLegacy()) {
                operateOnTypes.push("Attachments");
            }
        }

        if (operateOnTypes.length === 0) {
            operateOnTypes.push("None");
        }

        const migrationSettings: Raven.Server.Smuggler.Migration.DatabaseMigrationSettings = {
            DatabaseName: this.resourceName(),
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Documents.Smuggler.DatabaseItemType,
            RemoveAnalyzers: this.removeAnalyzers(),
            ImportRavenFs: this.importRavenFs(),
            TransformScript: this.transformScript()
        };

        return {
            ServerUrl: this.serverUrl(),
            MigrationSettings: migrationSettings,
            UserName: this.showWindowsCredentialInputs() ? this.userName() : null,
            Password: this.showWindowsCredentialInputs() ? this.password() : null, 
            Domain: this.showWindowsCredentialInputs() ? this.domain() : null, 
            ApiKey: this.showApiKeyCredentialInputs() ? this.apiKey() : null, 
            EnableBasicAuthenticationOverUnsecuredHttp: this.apiKey() ? this.enableBasicAuthenticationOverUnsecuredHttp() : false, 
            BuildMajorVersion: this.serverMajorVersion(),
            BuildVersion: this.buildVersion(),
            SkipServerCertificateValidation: this.isSecuredConnection() ? this.skipServerCertificateValidation() : false
        };
    }

    private initObservables() {
        this.serverMajorVersionNumber = ko.pureComputed<string>(() => {
            const serverMajorVersion = this.serverMajorVersion();
            const buildVersionInt = this.buildVersion();
            const productVersion = this.productVersion();

            if (!serverMajorVersion || !buildVersionInt) {
                return null;
            }

            let majorVersion: string;
            let buildVersion = buildVersionInt.toString();
            switch (serverMajorVersion) {
            case "Unknown":
                return "Unknown";
            case "V2":
                majorVersion = "2.x";
                break;
            case "V30":
                majorVersion = "3.0";
                break;
            case "V35":
                majorVersion = "3.5";
                break;
            case "V4":
                majorVersion = productVersion;
                break;
            default:
                return null;
            }

            return `${majorVersion} (build: ${buildVersion})`;
        });

        this.isRavenDb = ko.pureComputed(() => {
            const version = this.serverMajorVersion();
            if (!version) {
                return false;
            }

            return version !== "Unknown";
        });

        this.isLegacy = ko.pureComputed(() => {
           const version = this.serverMajorVersion();
           return version === "V2" || version === "V30" || version === "V35";
        });

        this.isV41 = ko.pureComputed(() => {
            if (this.isLegacy()) {
                return false;
            }

            const buildVersion = this.buildVersion();
            return buildVersion >= 41000 || buildVersion === 41;
        });

        this.hasRavenFs = ko.pureComputed(() => {
            const version = this.serverMajorVersion();
            return version === "V30" || version === "V35";
        });

        this.ravenFsImport = ko.pureComputed(() => this.hasRavenFs() && this.importRavenFs());

        this.resourceTypeName = ko.pureComputed(() => this.ravenFsImport() ? "File system" : "Database");

        this.showWindowsCredentialInputs = ko.pureComputed(() => {
            const authMethod = this.authenticationMethod();
            return authMethod === "windows";
        });

        this.showApiKeyCredentialInputs = ko.pureComputed(() => {
            const authMethod = this.authenticationMethod();
            return authMethod === "apiKey";
        });

        this.isUnsecuredBasicAuthentication = ko.pureComputed(() => {
            const url = this.serverUrl();
            if (!url) {
                return false;
            }

            return this.hasUnsecuredBasicAuthenticationOption() && url.toLowerCase().startsWith("http://");
        });

        this.isSecuredConnection = ko.pureComputed(() => {
            const url = this.serverUrl();
            if (!url) {
                return false;
            }

            return url.toLowerCase().startsWith("https://");
        });

        this.includeDocuments.subscribe(documents => {
            if (!documents) {
                this.includeCounters(false);
                this.includeAttachments(false);
                this.includeLegacyAttachments(false);
            }
        });

        this.removeAnalyzers.subscribe(analyzers => {
            if (analyzers) {
                this.includeIndexes(true);
            }
        });

        this.includeIndexes.subscribe(indexes => {
            if (!indexes) {
                this.removeAnalyzers(false);
            }
        });
    }
    
    private initValidation() {
        this.serverUrl.extend({
            required: true,
            validUrl: true
        });

        this.resourceName.extend({
            validation: [
                {
                    validator: () => !this.hasRavenFs() || this.authorized(),
                    message: "Unauthorized access to the server, please enter your credentials"
                },
                {
                    validator: (value: string) => value,
                    message: "This field is required."
                }
            ]
        });
        
        this.userName.extend({
            required: {
                onlyIf: () => this.showWindowsCredentialInputs()
            }
        });
        
        this.password.extend({
            required: {
                onlyIf: () => this.showWindowsCredentialInputs()
            }
        });

        this.apiKey.extend({
            required: {
                onlyIf: () => this.showApiKeyCredentialInputs()
            }
        });

        this.importDefinitionHasIncludes = ko.pureComputed(() => {
            if (this.serverMajorVersion() === "V4") {
                return this.includeDatabaseRecord() || this.includeDocuments() || this.includeRevisionDocuments() || this.includeConflicts() ||
                    this.includeIndexes() || this.includeIdentities() || this.includeCompareExchange() || this.includeCounters();
            }

            const hasIncludes = this.includeDocuments() || this.includeIndexes() || this.includeLegacyAttachments() || this.includeRevisionDocuments();
            if (this.serverMajorVersion() === "V30" || this.serverMajorVersion() === "V35") {
                return hasIncludes || this.importRavenFs();
            }

            return hasIncludes;
        });

        this.importDefinitionHasIncludes.extend({
            validation: [
                {
                    validator: () => this.importDefinitionHasIncludes(),
                    message: "Note: At least one 'include' option must be checked..."
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            serverUrl: this.serverUrl,
            databaseName: this.resourceName,
            serverMajorVersion: this.serverMajorVersion, 
            userName: this.userName,
            password: this.password, 
            domain: this.domain,
            importDefinitionHasIncludes: this.importDefinitionHasIncludes
        });
        
        this.versionCheckValidationGroup = ko.validatedObservable({
            serverUrl: this.serverUrl
        });
    }

    createServerUrlAutoCompleter() {
        return ko.pureComputed(() => {
            const options = this.serverUrls();
            let key = this.serverUrl();

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => x.toLowerCase().includes(key));
            }

            return options;
        });
    }

    selectServerUrl(serverUrl: string) {
        this.serverUrl(serverUrl);
    }

    createResourceNamesAutoCompleter() {
        return ko.pureComputed(() => {
            const options = this.getResourceNames();
            let key = this.resourceName();

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => x.toLowerCase().includes(key));
            }

            return options;
        });
    }

    private getResourceNames(): string[] {
        if (!this.hasRavenFs()) {
            return this.databaseNames();
        }

        return this.importRavenFs() ? this.fileSystemNames() : this.databaseNames();
    }

    selectResourceName(resourceName: string) {
        this.resourceName(resourceName);
    }
}

export = migrateRavenDbDatabaseModel;

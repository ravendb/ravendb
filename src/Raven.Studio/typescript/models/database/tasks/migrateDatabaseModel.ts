/// <reference path="../../../../typings/tsd.d.ts"/>

type authenticationMethod = "windows" | "none";

class migrateDatabaseModel {
    serverUrl = ko.observable<string>();
    databaseName = ko.observable<string>();
    includeDocuments = ko.observable(true);
    includeConflicts = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeIdentities = ko.observable(true);
    includeCompareExchange = ko.observable(true);
    includeRevisionDocuments = ko.observable(true);
    includeLegacyAttachments = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    revisionsAreConfigured: KnockoutComputed<boolean>;

    authenticationMethod = ko.observable<authenticationMethod>("none");
    
    serverMajorVersion = ko.observable<Raven.Server.Smuggler.Migration.MajorVersion>("Unknown");
    buildVersion = ko.observable<number>();
    serverMajorVersionNumber = ko.pureComputed<string>(() => {
        if (!this.serverMajorVersion()) {
            return null;
        }

        let majorVersion: string;
        switch (this.serverMajorVersion()) {
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
                majorVersion = "4.0";
                break;
            default:
                return null;
        }

        return `${majorVersion} (build: ${this.buildVersion()})`;
    });
    
    userName = ko.observable<string>();
    password = ko.observable<string>();
    domain = ko.observable<string>();
    
    isRavenDb: KnockoutComputed<boolean>;
    isLegacy: KnockoutComputed<boolean>;
    showWindowsCredentialInputs: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup;
    importDefinitionHasIncludes: KnockoutComputed<boolean>;
    versionCheckValidationGroup: KnockoutValidationGroup;

    constructor() {
        this.initObservables();
        this.initValidation();
    }

    toDto(): Raven.Server.Smuggler.Migration.SingleDatabaseMigrationConfiguration {
        const operateOnTypes: Array<Raven.Client.Documents.Smuggler.DatabaseItemType> = [];
        if (this.includeDocuments()) {
            operateOnTypes.push("Documents");
        }
        if (this.includeConflicts() && !this.isLegacy()) {
            operateOnTypes.push("Conflicts");
        }
        if (this.includeIndexes()) {
            operateOnTypes.push("Indexes");
        }
        if (this.includeRevisionDocuments() && !this.isLegacy()) {
            operateOnTypes.push("RevisionDocuments");
        }
        if (this.includeLegacyAttachments() && this.isLegacy()) {
            operateOnTypes.push("LegacyAttachments");
        }
        if (this.includeIdentities() && !this.isLegacy()) {
            operateOnTypes.push("Identities");
        }
        if (this.includeCompareExchange() && !this.isLegacy()) {
            operateOnTypes.push("CmpXchg");
        }

        const migrationSettings: Raven.Server.Smuggler.Migration.DatabaseMigrationSettings = {
            DatabaseName: this.databaseName(),
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Documents.Smuggler.DatabaseItemType,
            RemoveAnalyzers: this.removeAnalyzers()
        };
        return {
            ServerUrl: this.serverUrl(),
            MigrationSettings: migrationSettings,
            UserName: this.showWindowsCredentialInputs() ? this.userName() : null,
            Password: this.showWindowsCredentialInputs() ? this.password() : null, 
            Domain: this.showWindowsCredentialInputs() ? this.domain() : null, 
            BuildMajorVersion: this.serverMajorVersion(),
            BuildVersion: this.buildVersion()
        };
    }

    private initObservables() {
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

        this.showWindowsCredentialInputs = ko.pureComputed(() => {
            const authMethod = this.authenticationMethod();
            return authMethod === "windows";
        });
    }
    
    private initValidation() {
        this.serverUrl.extend({
            required: true,
            validUrl: true
        });

        this.databaseName.extend({
            required: true
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

        this.importDefinitionHasIncludes = ko.pureComputed(() => {
            if (this.serverMajorVersion() === "V4") {
                return this.includeDocuments() || (this.includeRevisionDocuments() && this.revisionsAreConfigured()) || this.includeConflicts() ||
                    this.includeIndexes() || this.includeIdentities() || this.includeCompareExchange();
            }

            return this.includeDocuments() || this.includeIndexes() || this.includeLegacyAttachments();
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
            databaseName: this.databaseName,
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
}

export = migrateDatabaseModel;

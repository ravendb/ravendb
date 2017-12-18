/// <reference path="../../../../typings/tsd.d.ts"/>

type authenticationMethod = "windows" | "none";

class migrateDatabaseModel {
    serverUrl = ko.observable<string>();
    databaseName = ko.observable<string>();

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
    
    showAuthenticationMethods: KnockoutComputed<boolean>;
    showWindowsCredentialInputs: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup;
    versionCheckValidationGroup: KnockoutValidationGroup;

    constructor() {
        this.initObservables();
        this.initValidation();
    }

    toDto(): Raven.Server.Smuggler.Migration.SingleDatabaseMigrationConfiguration {
        return {
            ServerUrl: this.serverUrl(),
            DatabaseName: this.databaseName(),
            UserName: this.showWindowsCredentialInputs() ? this.userName() : null,
            Password: this.showWindowsCredentialInputs() ? this.password() : null, 
            Domain: this.showWindowsCredentialInputs() ? this.domain() : null, 
            BuildMajorVersion: this.serverMajorVersion(),
            BuildVersion: this.buildVersion()
        };
    }

    private initObservables() {
        this.showAuthenticationMethods = ko.pureComputed(() => {
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

        this.validationGroup = ko.validatedObservable({
            serverUrl: this.serverUrl,
            databaseName: this.databaseName,
            serverMajorVersion: this.serverMajorVersion, 
            userName: this.userName,
            password: this.password, 
            domain: this.domain
        });
        
        this.versionCheckValidationGroup = ko.validatedObservable({
            serverUrl: this.serverUrl
        });
    }
}

export = migrateDatabaseModel;

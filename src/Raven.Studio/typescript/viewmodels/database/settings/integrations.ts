import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import postgreSqlCredentialsModel = require("models/database/settings/postgreSqlCredentialsModel");
import getIntegrationsPostgreSqlCredentialsCommand = require("commands/database/settings/getIntegrationsPostgreSqlCredentialsCommand");
import getIntegrationsPostgreSqlSupportCommand = require("commands/database/settings/getIntegrationsPostgreSqlSupportCommand");
import saveIntegrationsPostgreSqlCredentialsCommand = require("commands/database/settings/saveIntegrationsPostgreSqlCredentialsCommand");
import deleteIntegrationsPostgreSqlCredentialsCommand = require("commands/database/settings/deleteIntegrationsPostgreSqlCredentialsCommand");
import licenseModel from "models/auth/licenseModel";

class integrations extends viewModelBase {

    view = require("views/database/settings/integrations.html");
    
    postgreSqlCredentials = ko.observableArray<string>([]);
    
    editedPostgreSqlCredentials = ko.observable<postgreSqlCredentialsModel>(null);

    //TODO
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    errorText: KnockoutComputed<string>;

    clientVersion = viewModelBase.clientVersion;
    isPostgreSqlSupportEnabled = ko.observable<boolean>();
    needsLicenseUpgrade = ko.observable<boolean>(false);
    
    spinners = {
        test: ko.observable<boolean>(false)
    };

    constructor() {
        super();
        this.bindToCurrentInstance("onConfirmDelete");
        this.initObservables();
    }
    
    private initObservables(): void {
        this.errorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
    }

    activate(args: any) {
        super.activate(args);
        const license = licenseModel.licenseStatus();
        
        if (!license.HasPostgreSqlIntegration && !license.HasPowerBI) {
            this.isPostgreSqlSupportEnabled(true);
            this.needsLicenseUpgrade(true);
            return ;
        }  
        
        return $.when<any>(this.getAllIntegrationsCredentials(), this.getPostgreSqlSupportStatus());
    }
    
    private getPostgreSqlSupportStatus(): JQueryPromise<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlServerStatus> {
        return new getIntegrationsPostgreSqlSupportCommand(this.activeDatabase())
            .execute()
            .done(result => this.isPostgreSqlSupportEnabled(result.Active))
    }
    
    private getAllIntegrationsCredentials(): JQueryPromise<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames> {
        return new getIntegrationsPostgreSqlCredentialsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames) => {
                const users = result.Users.map(x => x.Username);
                this.postgreSqlCredentials(_.sortBy(users, userName => userName.toLowerCase()));
            });
    }
    
    onConfirmDelete(username: string): void {
        this.confirmationMessage("Delete credentials?",
            `You're deleting PostgreSQL credentials for user: <br><ul><li><strong>${generalUtils.escapeHtml(username)}</strong></li></ul>`, {
                buttons: ["Cancel", "Delete"],
                html: true
            })
            .done(result => {
                if (result.can) {
                    this.deleteIntegrationCredentials(username);
                }
            });
    }

    private deleteIntegrationCredentials(username: string): void {
        new deleteIntegrationsPostgreSqlCredentialsCommand(this.activeDatabase(), username)
            .execute()
            .done(() => {
                this.getAllIntegrationsCredentials();
                this.onCloseEdit();
            });
    }

    onAddPostgreSqlCredentials(): void {
        eventsCollector.default.reportEvent("PostgreSQL credentials", "add-postgreSql-credentials");
        
        this.editedPostgreSqlCredentials(new postgreSqlCredentialsModel(() => this.clearTestResult()));
        this.clearTestResult();
    }

    onSavePostgreSqlCredentials(): void {
        const modelToSave = this.editedPostgreSqlCredentials();
        if (modelToSave) {
            if (!this.isValid(modelToSave.validationGroup)) {
                return;
            }
            
            new saveIntegrationsPostgreSqlCredentialsCommand(this.activeDatabase(), modelToSave.username(), modelToSave.password())
                .execute()
                .done(() => {
                    this.getAllIntegrationsCredentials();
                    this.editedPostgreSqlCredentials(null);
                });
        }
    }

    onTestPostgreSqlCredentials(): void {
        this.clearTestResult();
        const postgreSqlCredentials = this.editedPostgreSqlCredentials();

        if (postgreSqlCredentials) {
            if (this.isValid(postgreSqlCredentials.validationGroup)) {
                eventsCollector.default.reportEvent("PostgreSQL credentials", "test-connection");

                // TODO
                // this.spinners.test(true);
                // postgreSqlCredentials.testConnection(this.activeDatabase())
                //     .done((testResult) => this.testConnectionResult(testResult))
                //     .always(() => {
                //         this.spinners.test(false);
                //     });
            }
        }
    }

    onCloseEdit(): void {
        this.editedPostgreSqlCredentials(null);
    }

    private clearTestResult(): void {
        this.testConnectionResult(null);
    }
}

export = integrations

import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import postgreSqlCredentialsModel = require("models/database/settings/postgreSqlCredentialsModel");
import getIntegrationsCredentialsCommand = require("commands/database/settings/getIntegrationsCredentialsCommand");
import saveIntegrationsCredentialsCommand = require("commands/database/settings/saveIntegrationsCredentialsCommand");
import deleteIntegrationsCredentialsCommand = require("../../../commands/database/settings/deleteIntegrationsCredentialsCommand");

class integrations extends viewModelBase {
   
    postgreSqlCredentials = ko.observableArray<string>([]);
    
    editedPostgreSqlCredentials = ko.observable<postgreSqlCredentialsModel>(null);

    //TODO
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    errorText: KnockoutComputed<string>;
    
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
        return this.getAllIntegrationsCredentials();
    }
    
    private getAllIntegrationsCredentials(): JQueryPromise<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLUsernamesList> {
        return new getIntegrationsCredentialsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLUsernamesList) => {
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
        new deleteIntegrationsCredentialsCommand(this.activeDatabase(), username)
            .execute()
            .done(() => {
                this.getAllIntegrationsCredentials();
                this.onCloseEdit();
            });
    }

    onAddPostgreSqlCredentials(): void {
        eventsCollector.default.reportEvent("PostgreSQL credentials", "add-postgreSql-credentials");
        
        this.editedPostgreSqlCredentials(postgreSqlCredentialsModel.empty());
        
        this.editedPostgreSqlCredentials().username.subscribe(() => this.clearTestResult());
        this.editedPostgreSqlCredentials().password.subscribe(() => this.clearTestResult());
        this.clearTestResult();
    }

    onSavePostgreSqlCredentials(): void {
        const modelToSave = this.editedPostgreSqlCredentials();
        if (modelToSave) {
            if (!this.isValid(modelToSave.validationGroup)) {
                return;
            }
            
            new saveIntegrationsCredentialsCommand(this.activeDatabase(), modelToSave.toDto())
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

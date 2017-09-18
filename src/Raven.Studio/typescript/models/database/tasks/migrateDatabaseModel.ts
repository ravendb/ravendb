/// <reference path="../../../../typings/tsd.d.ts"/>

class migrateDatabaseModel {
    serverUrl = ko.observable<string>();
    databaseName = ko.observable<string>();
    userName = ko.observable<string>();
    password = ko.observable<string>();
    domain = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;

    constructor() {
        this.initValidation();
    }

    toDto(): Raven.Server.Smuggler.Migration.SingleDatabaseMigrationConfiguration {
        return {
            ServerUrl: this.serverUrl(),
            DatabaseName: this.databaseName(),
            UserName: this.userName(),
            Password: this.password(),
            Domain: this.domain()
        };
    }

    private initValidation() {
        const urlError = ko.observable<string>();
        this.serverUrl.extend({
            required: true,
            validation: [
                {
                    validator: (nodeUrl: string) => {
                        try {
                            new URL(nodeUrl);
                            return true;
                        } catch (e) {
                            urlError((e as Error).message);
                            return false;
                        }
                    },
                    message: `{0}`,
                    params: urlError
                }
            ]
        });

        this.databaseName.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            serverUrl: this.serverUrl,
            databaseName: this.databaseName
        });
    }
}

export = migrateDatabaseModel;

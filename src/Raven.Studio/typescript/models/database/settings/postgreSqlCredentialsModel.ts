/// <reference path="../../../../typings/tsd.d.ts"/>

class postgreSqlCredentialsModel {

    username = ko.observable<string>();
    password = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLNewUser) {
        this.username(dto.Username);
        this.password(dto.Password);
        
        this.initValidation();
    }

    initValidation(): void {
        this.username.extend({
            required: true
        });

        this.password.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            userName: this.username,
            password: this.password
        });
    }

    static empty(): postgreSqlCredentialsModel {
        return new postgreSqlCredentialsModel({
            Username: null,
            Password: null
        });
    }
    
    toDto(): Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLNewUser {
        return {
            Username: this.username(),
            Password: this.password()
        };
    }
}

export = postgreSqlCredentialsModel;

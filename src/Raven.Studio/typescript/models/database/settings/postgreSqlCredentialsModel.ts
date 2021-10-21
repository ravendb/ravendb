/// <reference path="../../../../typings/tsd.d.ts"/>

class postgreSqlCredentialsModel {

    username = ko.observable<string>();
    password = ko.observable<string>();
    
    clearMethod: () => void;

    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLNewUser, clearMethod: () => void) {
        this.username(dto.Username);
        this.password(dto.Password);
        
        this.clearMethod = clearMethod;
        
        this.initObservables(); 
        this.initValidation();
    }

    private initObservables(): void {
        this.username.subscribe(() => this.clearMethod());
        this.password.subscribe(() => this.clearMethod());
    }
    
    private initValidation(): void {
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

    static empty(clearMethod: () => void): postgreSqlCredentialsModel {
        return new postgreSqlCredentialsModel({
            Username: null,
            Password: null
        }, clearMethod);
    }
    
    toDto(): Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLNewUser {
        return {
            Username: this.username(),
            Password: this.password()
        };
    }
}

export = postgreSqlCredentialsModel;

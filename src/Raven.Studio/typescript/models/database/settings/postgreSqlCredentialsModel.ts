/// <reference path="../../../../typings/tsd.d.ts"/>

class postgreSqlCredentialsModel {

    username = ko.observable<string>();
    password = ko.observable<string>();
    
    clearMethod: () => void;

    validationGroup: KnockoutValidationGroup;

    constructor(clearMethod: () => void) {
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
}

export = postgreSqlCredentialsModel;

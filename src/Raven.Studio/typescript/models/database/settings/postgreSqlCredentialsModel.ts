/// <reference path="../../../../typings/tsd.d.ts"/>
import generateSecretCommand = require("commands/database/secrets/generateSecretCommand");
import copyToClipboard = require("common/copyToClipboard");

class postgreSqlCredentialsModel {

    username = ko.observable<string>();
    password = ko.observable<string>();
    
    passwordHidden = ko.observable<boolean>(true);
    
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
    
    generatePassword(): JQueryPromise<string> {
        return new generateSecretCommand()
            .execute()
            .done(secret => this.password(secret));
    }
    
    copyPasswordToClipboard(): void {
        copyToClipboard.copy(this.password(), "Password has been copied to clipboard");
    }

    toggleHidden(): void {
        this.passwordHidden.toggle();
    }
}

export = postgreSqlCredentialsModel;

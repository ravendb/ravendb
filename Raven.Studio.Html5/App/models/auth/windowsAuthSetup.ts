import windowsAuthData = require("models/auth/windowsAuthData");
import verifyPrincipalCommand = require("commands/auth/verifyPrincipalCommand");

class windowsAuthSetup {

    static lastQueryId = 0; // used to discard non-latest responses, at verify-principal can take even 20 seconds :/

    static ModeUser = "user";
    static ModeGroup = "group";

    requiredUsers = ko.observableArray<windowsAuthData>();
    requiredGroups = ko.observableArray<windowsAuthData>();
    
    constructor(dto: windowsAuthDto) {
        this.requiredUsers(dto.RequiredUsers.map(winAuthDto => new windowsAuthData(winAuthDto)));
        this.requiredGroups(dto.RequiredGroups.map(winAuthDto => new windowsAuthData(winAuthDto)));
        
        this.requiredUsers().forEach((data: windowsAuthData) => windowsAuthSetup.subscribeToObservableName(data, this.requiredUsers, windowsAuthSetup.ModeUser));
        this.requiredGroups().forEach((data: windowsAuthData) => windowsAuthSetup.subscribeToObservableName(data, this.requiredGroups, windowsAuthSetup.ModeGroup));
    }

    toDto(): windowsAuthDto {
        return {
            RequiredGroups: this.requiredGroups().map(grp => grp.toDto()),
            RequiredUsers: this.requiredUsers().map(usr => usr.toDto())
        }
    }

    public static subscribeToObservableName(data: windowsAuthData, observableArray: KnockoutObservableArray<windowsAuthData>, mode: string) {
        data.name.subscribe((previousName) => {
            var existingWindowsAuthDataExceptCurrent = observableArray().filter((w: windowsAuthData) => w !== data && w.name() == previousName);
            if (existingWindowsAuthDataExceptCurrent.length == 1) {
                existingWindowsAuthDataExceptCurrent[0].nameCustomValidity('');
            }
        }, this, "beforeChange");
        data.name.throttle(500).subscribe(name => {

            var errorMessage: string = '';
            var isApiKeyNameValid = name.indexOf("\\") > 0;
            var existingApiKeys = observableArray().filter((w: windowsAuthData) => w !== data && w.name() == name);

            if (isApiKeyNameValid == false) {
                errorMessage = "Name must contain '\\'";
            } else if (existingApiKeys.length > 0) {
                errorMessage = "Name already exists!";
            }

            data.nameCustomValidity(errorMessage);

            if (!errorMessage) {
                data.verificationInProgress(true);
                data.invalidName(false);
                var queryId = ++windowsAuthSetup.lastQueryId;
                new verifyPrincipalCommand(mode, name)
                    .execute()
                    .done((valid: boolean) => {
                        if (windowsAuthSetup.lastQueryId === queryId) {
                            data.invalidName(!valid);
                        }
                    })
                    .always(() => {
                        if (windowsAuthSetup.lastQueryId === queryId) {
                            data.verificationInProgress(false);
                        }
                    });
            } else {
                data.invalidName(false);
            }
        });
    }
}
export = windowsAuthSetup;

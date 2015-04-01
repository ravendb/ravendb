import windowsAuthData = require("models/auth/windowsAuthData");

class windowsAuthSetup {

    requiredUsers = ko.observableArray<windowsAuthData>();
    requiredGroups = ko.observableArray<windowsAuthData>();
    
    constructor(dto: windowsAuthDto) {
        this.requiredUsers(dto.RequiredUsers.map(winAuthDto => new windowsAuthData(winAuthDto)));
        this.requiredGroups(dto.RequiredGroups.map(winAuthDto => new windowsAuthData(winAuthDto)));
        
        this.requiredUsers().forEach((data: windowsAuthData) => windowsAuthSetup.subscribeToObservableName(data, this.requiredUsers));
        this.requiredGroups().forEach((data: windowsAuthData) => windowsAuthSetup.subscribeToObservableName(data, this.requiredGroups));
    }

    toDto(): windowsAuthDto {
        return {
            RequiredGroups: this.requiredGroups().map(grp => grp.toDto()),
            RequiredUsers: this.requiredUsers().map(usr => usr.toDto())
        }
    }

    public static subscribeToObservableName(data: windowsAuthData, observableArray: KnockoutObservableArray<windowsAuthData>) {
        data.name.subscribe((previousName) => {
            var existingWindowsAuthDataExceptCurrent = observableArray().filter((w: windowsAuthData) => w !== data && w.name() == previousName);
            if (existingWindowsAuthDataExceptCurrent.length == 1) {
                existingWindowsAuthDataExceptCurrent[0].nameCustomValidity('');
            }
        }, this, "beforeChange");
        data.name.subscribe((newName) => {
            var errorMessage: string = '';
            var isApiKeyNameValid = newName.indexOf("\\") > 0;
            var existingApiKeys = observableArray().filter((w: windowsAuthData) => w !== data && w.name() == newName);

            if (isApiKeyNameValid == false) {
                errorMessage = "Name must contain '\\'";
            } else if (existingApiKeys.length > 0) {
                errorMessage = "Name already exists!";
            }

            data.nameCustomValidity(errorMessage);
        });
    }
}
export = windowsAuthSetup;
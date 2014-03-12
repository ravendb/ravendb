import windowsAuthData = require("models/windowsAuthData");

class windowsAuthSetup {

    requiredGroups = ko.observableArray<windowsAuthData>();
    requiredUsers = ko.observableArray<windowsAuthData>();

    constructor(dto: windowsAuthDto) {
        this.requiredGroups(dto.RequiredGroups.map(winAuthDto => new windowsAuthData(winAuthDto)));
        this.requiredUsers(dto.RequiredUsers.map(winAuthDto => new windowsAuthData(winAuthDto)));
    }

    toDto(): windowsAuthDto {
        return {
            RequiredGroups: this.requiredGroups().map(grp => grp.toDto()),
            RequiredUsers: this.requiredUsers().map(usr => usr.toDto())
        }
    }
}
export = windowsAuthSetup;
import windowsAuthSetup = require("models/windowsAuthSetup");
import windowsAuthData = require("models/windowsAuthData");
import viewModelBase = require("viewmodels/viewModelBase");
import getWindowsAuthCommand = require("commands/getWindowsAuthCommand");
import saveWindowsAuthCommand = require("commands/saveWindowsAuthCommand");

class windowsAuth {

    setup = ko.observable<windowsAuthSetup>();

    activate() {
        this.setup(new windowsAuthSetup({ RequiredUsers: [], RequiredGroups: [] }));

        new getWindowsAuthCommand()
            .execute()
            .done(result => this.setup(result));
    }

    saveChanges() {
        new saveWindowsAuthCommand(this.setup().toDto()).execute();
    }

    addUserSettings() {
        this.setup().requiredUsers.push(windowsAuthData.empty());
    }

    removeUserSettings(data: windowsAuthData) {
        this.setup().requiredUsers.remove(data);
    }

    addGroupSettings() {
        this.setup().requiredGroups.push(windowsAuthData.empty());
    }

    removeGroupSettings(data: windowsAuthData) {
        this.setup().requiredGroups.remove(data);
    }

}

export = windowsAuth;
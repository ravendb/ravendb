import windowsAuthSetup = require("models/windowsAuthSetup");
import windowsAuthData = require("models/windowsAuthData");
import viewModelBase = require("viewmodels/viewModelBase");
import getWindowsAuthCommand = require("commands/getWindowsAuthCommand");
import saveWindowsAuthCommand = require("commands/saveWindowsAuthCommand");

class windowsAuth extends viewModelBase {

    setup = ko.observable<windowsAuthSetup>().extend({ required: true });
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args) {
        super.canActivate(args);

        var deffered = $.Deferred();
        this.setup(new windowsAuthSetup({ RequiredUsers: [], RequiredGroups: [] }));
        this.fetchWindowsAuth().always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    activate(args) {
        super.activate(args);

        this.dirtyFlag = new ko.DirtyFlag([this.setup]);
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    private fetchWindowsAuth(): JQueryPromise<any> {
        return new getWindowsAuthCommand()
            .execute()
            .done(result => this.setup(result));
    }

    saveChanges() {
        new saveWindowsAuthCommand(this.setup().toDto())
            .execute()
            .done(() => this.dirtyFlag().reset());
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
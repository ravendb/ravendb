import windowsAuthSetup = require("models/auth/windowsAuthSetup");
import windowsAuthData = require("models/auth/windowsAuthData");
import viewModelBase = require("viewmodels/viewModelBase");
import getWindowsAuthCommand = require("commands/auth/getWindowsAuthCommand");
import saveWindowsAuthCommand = require("commands/auth/saveWindowsAuthCommand");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class windowsAuth extends viewModelBase {

    setup = ko.observable<windowsAuthSetup>();
    isSaveEnabled: KnockoutComputed<boolean>;
    isUsersSectionActive = ko.observable<boolean>(true);

    settingsAccess = new settingsAccessAuthorizer();

    canActivate(args) {
        var deferred = $.Deferred();

        if (this.settingsAccess.isForbidden()) {
            deferred.resolve({ can: true });           
        } else {
            this.setup(new windowsAuthSetup({ RequiredUsers: [], RequiredGroups: [] }));
            this.fetchWindowsAuth().always(() => deferred.resolve({ can: true }));
        }

        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('ZDGUY9');

        this.dirtyFlag = new ko.DirtyFlag([this.setup]);
        this.isSaveEnabled = ko.computed(() => !this.settingsAccess.isReadOnly() && this.dirtyFlag().isDirty());
    }

    compositionComplete() {
        super.compositionComplete();
        if (this.settingsAccess.isReadOnly()) {
            $('#manageWindowsAuth form input').attr('readonly', 'readonly');
            $('#manageWindowsAuth button').attr('disabled', 'true');
        }
        $("#manageWindowsAuth form").on("keypress", 'input[name="databaseName"]', (e) => e.which !== 13);
    }

    private fetchWindowsAuth(): JQueryPromise<any> {
        return new getWindowsAuthCommand()
            .execute()
            .done((result: windowsAuthSetup) => this.setup(result));
    }

    saveChanges() {
        new saveWindowsAuthCommand(this.setup().toDto())
            .execute()
            .done(() => this.dirtyFlag().reset());
    }

    addUserSettings() {
        var newAuthData = windowsAuthData.empty();
        windowsAuthSetup.subscribeToObservableName(newAuthData, this.setup().requiredUsers, windowsAuthSetup.ModeUser);
        this.setup().requiredUsers.push(newAuthData);
    }

    removeUserSettings(data: windowsAuthData) {
        this.setup().requiredUsers.remove(data);
    }

    addGroupSettings() {
        var newAuthData = windowsAuthData.empty();
        windowsAuthSetup.subscribeToObservableName(newAuthData, this.setup().requiredGroups, windowsAuthSetup.ModeGroup);
        this.setup().requiredGroups.push(newAuthData);
    }

    removeGroupSettings(data: windowsAuthData) {
        this.setup().requiredGroups.remove(data);
    }
}

export = windowsAuth;
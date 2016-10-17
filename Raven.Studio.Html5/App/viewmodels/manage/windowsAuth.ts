import windowsAuthSetup = require("models/auth/windowsAuthSetup");
import windowsAuthData = require("models/auth/windowsAuthData");
import viewModelBase = require("viewmodels/viewModelBase");
import getWindowsAuthCommand = require("commands/auth/getWindowsAuthCommand");
import saveWindowsAuthCommand = require("commands/auth/saveWindowsAuthCommand");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import eventsCollector = require("common/eventsCollector");

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
            $('#manageWindowsAuth input').attr('readonly', 'readonly');
            $('#manageWindowsAuth button').attr('disabled', 'true');
        }
        $("#manageWindowsAuth").on("keypress", 'input[name="databaseName"]', (e) => e.which !== 13);
    }

    private fetchWindowsAuth(): JQueryPromise<any> {
        return new getWindowsAuthCommand()
            .execute()
            .done((result: windowsAuthSetup) => this.setup(result));
    }

    saveChanges() {
        eventsCollector.default.reportEvent("windows-auth", "save");
        new saveWindowsAuthCommand(this.setup().toDto())
            .execute()
            .done(() => this.dirtyFlag().reset());
    }

    addUserSettings() {
        eventsCollector.default.reportEvent("windows-auth", "add-user-settings");
        var newAuthData = windowsAuthData.empty();
        windowsAuthSetup.subscribeToObservableName(newAuthData, this.setup().requiredUsers, windowsAuthSetup.ModeUser);
        this.setup().requiredUsers.push(newAuthData);
    }

    removeUserSettings(data: windowsAuthData) {
        eventsCollector.default.reportEvent("windows-auth", "remove-user-settings");
        this.setup().requiredUsers.remove(data);
    }

    addGroupSettings() {
        eventsCollector.default.reportEvent("windows-auth", "add-group-settings");
        var newAuthData = windowsAuthData.empty();
        windowsAuthSetup.subscribeToObservableName(newAuthData, this.setup().requiredGroups, windowsAuthSetup.ModeGroup);
        this.setup().requiredGroups.push(newAuthData);
    }

    removeGroupSettings(data: windowsAuthData) {
        eventsCollector.default.reportEvent("windows-auth", "remove-group-settings");
        this.setup().requiredGroups.remove(data);
    }
}

export = windowsAuth;
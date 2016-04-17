import windowsAuthSetup = require("models/auth/windowsAuthSetup");
import windowsAuthData = require("models/auth/windowsAuthData");
import viewModelBase = require("viewmodels/viewModelBase");
import getWindowsAuthCommand = require("commands/auth/getWindowsAuthCommand");
import saveWindowsAuthCommand = require("commands/auth/saveWindowsAuthCommand");
import shell = require("viewmodels/shell");

class windowsAuth extends viewModelBase {

    setup = ko.observable<windowsAuthSetup>().extend({ required: true });
    isSaveEnabled: KnockoutComputed<boolean>;
    isUsersSectionActive = ko.observable<boolean>(true);
    isForbidden = ko.observable<boolean>();
    isReadOnly: KnockoutComputed<boolean>;

    canActivate(args) {
        var deferred = $.Deferred();

        this.isForbidden((shell.isGlobalAdmin() || shell.canReadWriteSettings() || shell.canReadSettings()) === false);
        this.isReadOnly = ko.computed(() => shell.isGlobalAdmin() === false && shell.canReadWriteSettings() === false && shell.canReadSettings());

        if (this.isForbidden() === false) {
            this.setup(new windowsAuthSetup({ RequiredUsers: [], RequiredGroups: [] }));
            this.fetchWindowsAuth().always(() => deferred.resolve({ can: true }));
        } else {
            deferred.resolve({ can: true });
        }

        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('ZDGUY9');

        this.dirtyFlag = new ko.DirtyFlag([this.setup]);
        this.isSaveEnabled = ko.computed(() => this.isReadOnly() === false && this.dirtyFlag().isDirty());
    }

    compositionComplete() {
        super.compositionComplete();
        if (this.isReadOnly()) {
            $('form input').attr('readonly', 'readonly');
            $('button').attr('disabled', 'true');
        }
        $("form").on("keypress", 'input[name="databaseName"]', (e) => e.which != 13);
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
import appUrl = require("common/appUrl");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import router = require("plugins/router");
import app = require("durandal/app");
import changesApi = require("common/changesApi");
import viewSystemDatabaseConfirm = require("viewmodels/viewSystemDatabaseConfirm");
import shell = require("viewmodels/shell");
import changesCallback = require("common/changesCallback");
import changeSubscription = require("models/changeSubscription");
import uploadItem = require("models/uploadItem");
import ace = require("ace/ace");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
class viewModelBase {
    public activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);
    public activeFilesystem = ko.observable<filesystem>().subscribeTo("ActivateFilesystem", true);

    private keyboardShortcutDomContainers: string[] = [];
    private modelPollingHandle: number;
    private notifications: Array<changeSubscription> = [];
    static dirtyFlag = new ko.DirtyFlag([]);
    private static isConfirmedUsingSystemDatabase: boolean;

    /*
     * Called by Durandal when checking whether this navigation is allowed. 
     * Possible return values: boolean, promise<boolean>, {redirect: 'some/other/route'}, promise<{redirect: 'some/other/route'}>
     * 
     * We use this to determine whether we should allow navigation to a system DB page.
     * p.s. from Judah: a big scary prompt when loading the system DB is a bit heavy-handed, no? 
     */
    canActivate(args: any): any {
        var database = (appUrl.getDatabase() != null) ? appUrl.getDatabase() : appUrl.getSystemDatabase(); //TODO: temporary fix for routing problem for system databse - remove this when fixed
        var filesystem = appUrl.getFilesystem();

        // we only want to prompt warning to system db if we are in the databases section, not in the filesystems one
        if (database.isSystem && filesystem.isDefault) {
            if (viewModelBase.isConfirmedUsingSystemDatabase) {
                return true;
            }

            return this.promptNavSystemDb();
        }

        viewModelBase.isConfirmedUsingSystemDatabase = false;

        return true;
    }

    createNotifications(): Array<changeSubscription> {
        return [];
    }

    cleanupNotifications() {
        for (var i = 0; i < this.notifications.length; i++) {
            this.notifications[i].off();
        }
        this.notifications = [];
    }

    /*
    * Called by Durandal when the view model is loaded and before the view is inserted into the DOM.
    */
    activate(args) {
        var db = appUrl.getDatabase();
        var currentDb = this.activeDatabase();
        if (!!db && db !== null && (!currentDb || currentDb.name !== db.name)) {
            ko.postbox.publish("ActivateDatabaseWithName", db.name);
        }
        this.notifications = this.createNotifications();

        var fs = appUrl.getFilesystem();
        var currentFilesystem = this.activeFilesystem();
        if (!currentFilesystem || currentFilesystem.name !== fs.name) {
            ko.postbox.publish("ActivateFilesystemWithName", fs.name);
        }

        this.modelPollingStart();
        window.onbeforeunload = (e: any) => this.beforeUnload(e); ko.postbox.publish("SetRawJSONUrl", "");
    }

    // Called back after the entire composition has finished (parents and children included)
    compositionComplete() {
        viewModelBase.dirtyFlag().reset(); //Resync Changes
    }

    /*
    * Called by Durandal before deactivate in order to determine whether removing from the DOM is necessary.
    */
    canDeactivate(isClose): any {
        var isDirty = viewModelBase.dirtyFlag().isDirty();
        if (isDirty) {
            return app.showMessage('You have unsaved data. Are you sure you want to close?', 'Unsaved Data', ['Yes', 'No']);
        }
        return true;
    }

    /*
     * Called by Durandal when the view model is unloading and the view is about to be removed from the DOM.
     */
    deactivate() {
        this.activeDatabase.unsubscribeFrom("ActivateDatabase");
        this.activeFilesystem.unsubscribeFrom("ActivateFilesystem");
        this.keyboardShortcutDomContainers.forEach(el => this.removeKeyboardShortcuts(el));
        this.modelPollingStop();
        this.cleanupNotifications();
    }
    /*
     * Creates a keyboard shortcut local to the specified element and its children.
     * The shortcut will be removed as soon as the view model is deactivated.
     * Also defines shortcut for ace editor, if ace editor was received
     */
    createKeyboardShortcut(keys: string, handler: () => void, elementSelector: string) {
        jwerty.key(keys, e => {
            e.preventDefault();
            handler();
        }, this, elementSelector);

        if (!this.keyboardShortcutDomContainers.contains(elementSelector)) {
            this.keyboardShortcutDomContainers.push(elementSelector);
        }
    }

    private removeKeyboardShortcuts(elementSelector: string) {
        $(elementSelector).unbind('keydown.jwerty');
    }

    /*
     * Navigates to the specified URL, recording a navigation event in the browser's history.
     */
    navigate(url: string) {
        router.navigate(url);
    }

    /*
     * Navigates by replacing the current URL. It does not record a new entry in the browser's navigation history.
     */
    updateUrl(url: string) {
        var options: DurandalNavigationOptions = {
            replace: true,
            trigger: false
        };
        router.navigate(url, options);
    }

    modelPolling() { 
    }

    forceModelPolling() {  
        this.modelPolling();
    }

    modelPollingStart() {
        this.modelPolling();
        this.modelPollingHandle = setInterval(() => this.modelPolling(), 5000);
        this.activeDatabase.subscribe(() => this.forceModelPolling());
        this.activeFilesystem.subscribe(() => this.forceModelPolling());
    }

    modelPollingStop() {
        clearInterval(this.modelPollingHandle);
    }

    private promptNavSystemDb(): any {
        if (!appUrl.warnWhenUsingSystemDatabase) {
            return true;
        }

        var canNavTask = $.Deferred<any>();

        var systemDbConfirm = new viewSystemDatabaseConfirm("Meddling with the system database could cause irreversible damage");
        systemDbConfirm.viewTask
            .fail(() => canNavTask.resolve({ redirect: 'databases' }))
            .done(() => {
                viewModelBase.isConfirmedUsingSystemDatabase = true;
                canNavTask.resolve(true);
            });
        app.showDialog(systemDbConfirm);

        return canNavTask;
    }



    private beforeUnload(e: any) {
        var isDirty = viewModelBase.dirtyFlag().isDirty();
        if (isDirty) {
            var message = "You have unsaved data.";
            e = e || window.event;

            // For IE and Firefox
            if (e) {
                e.returnValue = message;
            }

            // For Safari
            return message;
        }
    }
}

export = viewModelBase;
import appUrl = require("common/appUrl");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import router = require("plugins/router");
import app = require("durandal/app");
import changesApi = require("common/changesApi");
import viewSystemDatabaseConfirm = require("viewmodels/viewSystemDatabaseConfirm");
import shell = require("viewmodels/shell");
import changesCallback = require("common/changesCallback");
import changeSubscription = require("models/changeSubscription");
import uploadItem = require("models/uploadItem");
import ace = require("ace/ace");
import oauthContext = require("common/oauthContext");
import messagePublisher = require("common/messagePublisher");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
class viewModelBase {
    public activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);
    public activeFilesystem = ko.observable<filesystem>().subscribeTo("ActivateFilesystem", true);
    public activeCounterStorage = ko.observable<counterStorage>().subscribeTo("ActivateCounterStorage", true);

    private keyboardShortcutDomContainers: string[] = [];
    static modelPollingHandle: number; // mark as static to fix https://github.com/BlueSpire/Durandal/issues/181
    private notifications: Array<changeSubscription> = [];
    private postboxSubscriptions: Array<KnockoutSubscription> = [];
    private static isConfirmedUsingSystemDatabase: boolean;
    dirtyFlag = new ko.DirtyFlag([]);

    /*
     * Called by Durandal when checking whether this navigation is allowed. 
     * Possible return values: boolean, promise<boolean>, {redirect: 'some/other/route'}, promise<{redirect: 'some/other/route'}>
     * 
     * We use this to determine whether we should allow navigation to a system DB page.
     * p.s. from Judah: a big scary prompt when loading the system DB is a bit heavy-handed, no? 
     */
    canActivate(args: any): any {
        var db = this.activeDatabase();

        // we only want to prompt warning to system db if we are in the databases section
        if (!!db && db.isSystem) {
            if (viewModelBase.isConfirmedUsingSystemDatabase) {
                return true;
            }

            return this.promptNavSystemDb();
        }
        else if (!!db && db.disabled()) {
            messagePublisher.reportError("Database '" + db.name + "' is disabled!", "You can't access any section of the database when it's disabled.");
            return { redirect: appUrl.forDatabases() };
        }

        viewModelBase.isConfirmedUsingSystemDatabase = false;
        
        return true;
    }

    /*
     * Called by Durandal when the view model is loaded and before the view is inserted into the DOM.
     */
    activate(args) {
        var db = appUrl.getDatabase();
        var currentDb = this.activeDatabase();
        if (!!db && (!currentDb || currentDb.name !== db.name)) {
            ko.postbox.publish("ActivateDatabaseWithName", db.name);
        }

        oauthContext.enterApiKeyTask.done(() => this.notifications = this.createNotifications());

        this.postboxSubscriptions = this.createPostboxSubscriptions();
        this.modelPollingStart();

        window.addEventListener("beforeunload", this.beforeUnloadListener, false);

        ko.postbox.publish("SetRawJSONUrl", "");
    }

    /*
     * Called by Durandal when the view model is loaded and after the view is inserted into the DOM.
     */
    compositionComplete() {
        this.dirtyFlag().reset(); //Resync Changes
    }

    /*
     * Called by Durandal before deactivate in order to determine whether removing from the DOM is necessary.
     */
    canDeactivate(isClose): any {
        var isDirty = this.dirtyFlag().isDirty();
        if (isDirty) {
            return this.confirmationMessage('Unsaved Data', 'You have unsaved data. Are you sure you want to continue?', undefined, true);
        }
        return true;
    }
    
    /*
     * Called by Durandal when the view model is unloaded and after the view is removed from the DOM.
     */
    detached() {
        this.cleanupNotifications();
        this.cleanupPostboxSubscriptions();
        window.removeEventListener("beforeunload", this.beforeUnloadListener, false);
    }

    /*
     * Called by Durandal when the view model is unloading and the view is about to be removed from the DOM.
     */
    deactivate() {
        this.activeDatabase.unsubscribeFrom("ActivateDatabase");
        this.activeFilesystem.unsubscribeFrom("ActivateFilesystem");
        this.activeCounterStorage.unsubscribeFrom("ActivateCounterStorage");
        this.keyboardShortcutDomContainers.forEach(el => this.removeKeyboardShortcuts(el));
        this.modelPollingStop();
    }

    createNotifications(): Array<changeSubscription> {
        return [];
    }

    cleanupNotifications() {
        this.notifications.forEach((notification: changeSubscription) => notification.off());
        this.notifications = [];
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [];
    }

    cleanupPostboxSubscriptions() {
        this.postboxSubscriptions.forEach((subscription: KnockoutSubscription) => subscription.dispose());
        this.postboxSubscriptions = [];
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
        // clear previous pooling handle (if any)
        if (viewModelBase.modelPollingHandle) {
            this.modelPollingStop();
            viewModelBase.modelPollingHandle = null;
        }
        viewModelBase.modelPollingHandle = setInterval(() => this.modelPolling(), 5000);
        this.activeDatabase.subscribe(() => this.forceModelPolling());
        this.activeFilesystem.subscribe(() => this.forceModelPolling());
    }

    modelPollingStop() {
        clearInterval(viewModelBase.modelPollingHandle);
    }

    confirmationMessage(title: string, confirmationMessage: string, options: string[]= ['No', 'Yes'], forceRejectWithResolve: boolean = false): JQueryPromise<any> {
        var viewTask = $.Deferred();
        var messageView = app.showMessage(confirmationMessage, title, options);

        messageView.done((answer) => {
            if (answer == options[1]) {
                viewTask.resolve({ can: true });
            } else if (!forceRejectWithResolve) {
                viewTask.reject();
            } else {
                viewTask.resolve({ can: false });
            }
        });

        return viewTask;
    }

    canContinueIfNotDirty(title: string, confirmationMessage: string, options: string[]= ['Yes', 'No']) {
        var deferred = $.Deferred();

        var isDirty = this.dirtyFlag().isDirty();
        if (isDirty) {
            var confirmationMessageViewModel = this.confirmationMessage(title, confirmationMessage, options);
            confirmationMessageViewModel.done(() => deferred.resolve());
        } else {
            deferred.resolve();
        }

        return deferred;
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

    private beforeUnloadListener: EventListener = (e: any): any => {
        var isDirty = this.dirtyFlag().isDirty();
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

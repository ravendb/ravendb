import appUrl = require("common/appUrl");
import database = require("models/database");
import router = require("plugins/router");
import app = require("durandal/app");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
class viewModelBase {
    activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);
    private keyboardShortcutDomContainers: string[] = [];
    private modelPollingHandle: number;
    static dirtyFlag = new ko.DirtyFlag([]);
    
    /*
     * Called by Durandal when checking whether this navigation is allowed. 
     * Possible return values: boolean, promise<boolean>, {redirect: 'some/other/route'}, promise<{redirect: 'some/other/route'}>
     * 
     * We use this to determine whether we should allow navigation to a system DB page.
     * p.s. from Judah: a big scary prompt when loading the system DB is a bit heavy-handed, no? 
     */
    canActivate(args: any): any {
        // See if we're on the system database. If so, we'll may to prompt before continuing.
        var activeDb = this.activeDatabase();
        if (activeDb && activeDb.isSystem && appUrl.warnWhenUsingSystemDatabase) {
            return this.promptNavSystemDb();
        }

        return true;
    }

    /*
    * Called by Durandal when the view model is loaded and before the view is inserted into the DOM.
    */
    activate(args) {
        var db = appUrl.getDatabase();
        var currentDb = this.activeDatabase();
        if (!currentDb || currentDb.name !== db.name) {
            ko.postbox.publish("ActivateDatabaseWithName", db.name);
        }
		
        this.modelPollingStart();
        
        window.onbeforeunload = (e: any) => {
            this.saveInObservable();
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
            return null;
        };

        ko.postbox.publish("SetRawJSONUrl", "");
    }

    // Called back after the entire composition has finished (parents and children included)
    compositionComplete() {
        // Resync Changes
        viewModelBase.dirtyFlag().reset();
    }

    /*
    * Called by Durandal before deactivate in order to detemine whether removing from the DOM is necessary.
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
        this.keyboardShortcutDomContainers.forEach(el => this.removeKeyboardShortcuts(el));
        this.modelPollingStop();
    }

    /*
     * Creates a keyboard shortcut local to the specified element and its children.
     * The shortcut will be removed as soon as the view model is deactivated.
     * Also defines shortcut for ace edito, if ace editor was recieved
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
    
    //A method to save the current value in the observables from text boxes and inputs before a refresh/page close.
    //Should be implemented on the inhereting class.
    saveInObservable() {

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

    private modelPollingStart() {
        this.modelPolling();
        this.modelPollingHandle = setInterval(() => this.modelPolling(), 5000);
        this.activeDatabase.subscribe(() => this.forceModelPolling());
    }

    private modelPollingStop() {
        clearInterval(this.modelPollingHandle);
    }

    private promptNavSystemDb(): JQueryPromise<boolean> {
        var canNavTask = $.Deferred<boolean>();

        // Load the viewSystemDatabaseConfirm view model on demand.
        // We really don't need it until the user tries to navigate to the system DB.
        require(["viewmodels/viewSystemDatabaseConfirm"], (viewSystemDatabaseConfirm => {
            var systemDbConfirm = new viewSystemDatabaseConfirm();
            systemDbConfirm.viewTask
                .fail(() => canNavTask.resolve(false))
                .done(() => {
                    appUrl.warnWhenUsingSystemDatabase = false;
                    canNavTask.resolve(true);
                });
            app.showDialog(systemDbConfirm);
        }));
		
		return canNavTask;
    }
}

export = viewModelBase;
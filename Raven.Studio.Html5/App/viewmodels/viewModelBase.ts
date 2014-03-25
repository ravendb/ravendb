import appUrl = require("common/appUrl");
import database = require("models/database");
import router = require("plugins/router");
import app = require("durandal/app");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/

interface KnockoutStatic {
    DirtyFlag(any): void;
}

class viewModelBase {
    activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);
    private keyboardShortcutDomContainers: string[] = [];
    public dirtyFlag = new ko.DirtyFlag([]);
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
        
        var self = this;
        window.onbeforeunload = function (e: any) {

            var isDirty = self.dirtyFlag().isDirty();
            if (isDirty) {
                var message = "You have unsaved data.";
                var e = e || window.event;
                // For IE and Firefox
                if (e) {
                    e.returnValue = message;
                }
                // For Safari
                return message;
            }
            return null;
        };
    }

    /*
    * Called by Durandal before deactivate in order to detemine whether removing from the DOM is necessary.
    */
    canDeactivate(isClose): any{
        var isDirty = this.dirtyFlag().isDirty();
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

    //#region Model Polling

    modelPollingHandle: number;

    modelPollingStart() {
        this.modelPolling();
        this.modelPollingHandle = setInterval(() => this.modelPolling(), 5000);
        this.activeDatabase.subscribe(() => this.forceModelPolling());
    }

    modelPollingStop() {
        clearInterval(this.modelPollingHandle);
    }

    modelPolling() {
    }

    forceModelPolling() {
        this.modelPolling();
    }

  //#endregion Model Polling
}

export = viewModelBase;
import appUrl = require("common/appUrl");
import database = require("models/database");
import filesystem = require("models/filesystem");
import resource = require("models/resource");
import router = require("plugins/router");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
class viewModelBase {
    activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);
    activeFilesystem = ko.observable<filesystem>().subscribeTo("ActivateFilesystem", true);
    activeView = ko.observable<string>();

  private keyboardShortcutDomContainers: string[] = [];

     /*
     * Called by Durandal when the view model is loaded and before the view is inserted into the DOM.
     */
    activate(args) {

        var db = appUrl.getDatabase();
        var currentDb = this.activeDatabase();
        if (!currentDb || currentDb.name !== db.name) {
            ko.postbox.publish("ActivateDatabaseWithName", db.name);
        }

        var fs = appUrl.getFilesystem();
        var currentFilesystem = this.activeFilesystem();
        if (!currentFilesystem || currentFilesystem.name !== fs.name) {
            ko.postbox.publish("ActivateFilesystemWithName", fs.name);
        }
		
		this.modelPollingStart();
    }

    /*
     * Called by Durandal when the view model is unloading and the view is about to be removed from the DOM.
     */
    deactivate() {
        this.activeDatabase.unsubscribeFrom("ActivateDatabase");
        this.activeFilesystem.unsubscribeFrom("ActivateFilesystem");
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
      this.activeDatabase.subscribe(() => {
          this.activeView('Databases');
          this.forceModelPolling();
      });
      this.activeFilesystem.subscribe(() => {
          this.activeView('Filesystems');
          this.forceModelPolling();
      });
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
import appUrl = require("common/appUrl");
import database = require("models/database");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
class viewModelBase {
  activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);

  activate(args) {
    var db = appUrl.getDatabase();
    var currentDb = this.activeDatabase();
    if (!currentDb || currentDb.name !== db.name) {
      ko.postbox.publish("ActivateDatabaseWithName", db.name);
    }

    this.modelPollingStart();
  }

  deactivate() {
    this.activeDatabase.unsubscribeFrom("ActivateDatabase");
    this.modelPollingStop();
  }

  createKeyboardShortcut(keys: string, handler: ()=> void, elementSelector: string) {
    jwerty.key(keys, e=> {
      e.preventDefault();
      handler();
    }, this, elementSelector);
  }

  removeKeyboardShortcuts(elementSelector: string) {
    $(elementSelector).unbind('keydown.jwerty');
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
import commandBase = require("commands/commandBase");

interface Window {
  EventSource: {};
}

interface EventSourceMessageEvent {
  lastEventId: any;
}

class changesApiCommand extends commandBase {

    constructor() {
        super();
    }

    execute(): JQueryPromise<any> {
     /* if (!window.EventSource) {
        console.log('EventStore is not available');
        return;
      }

      var source = new EventSource(url);

      source.addEventListener('message', e => {
        // validate origin
        console.log(e.data);
      }, false);

      source.addEventListener('open', e => {
        // Connection was opened.
      }, false);

      source.addEventListener('error', e => {
        if (e.readyState == EventSource.CLOSED) {
          // Connection was closed.
        }
      }, false);*/
      return null;
    }
}

export = changesApiCommand; 
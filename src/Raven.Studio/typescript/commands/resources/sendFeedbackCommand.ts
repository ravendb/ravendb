import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class sendFeedbackCommand extends commandBase {

    constructor(private form: Raven.Server.Documents.Studio.FeedbackForm) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.studioFeedback.studioFeedback;
        return this.post<void>(url, JSON.stringify(this.form), null, { dataType: undefined })
            .done(() => this.reportSuccess("Feedback sent. Thank you."))
            .fail((response: JQueryXHR) => this.reportError("Failed to send feedback", response.responseText, response.statusText));
    }
}

export = sendFeedbackCommand;

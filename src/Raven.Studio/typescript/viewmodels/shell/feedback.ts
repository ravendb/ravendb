import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import sendFeedbackCommand = require("commands/resources/sendFeedbackCommand");
import dialog = require("plugins/dialog");

class feedbackModel {
    name = ko.observable<string>();
    email = ko.observable<string>();
    message = ko.observable<string>();

    validationGroup = ko.validatedObservable({
        name: this.name,
        email: this.email,
        message: this.message
    });

    constructor() {
        this.setupValidation();
    }

    private setupValidation() {
        this.email.extend({
            email: true
        });

        this.message.extend({
            required: true
        });
    }
}

class feedback extends dialogViewModelBase {

    private studioVersion: string;
    private serverVersion: string;

    model = new feedbackModel();

    spinners = {
        send: ko.observable<boolean>(false)
    }

    constructor(studioVersion: string, serverVersion: string) {
        super();
        this.studioVersion = studioVersion;
        this.serverVersion = serverVersion;
    }

    private toDto(): Raven.Server.Documents.Studio.FeedbackForm {
        return {
            Name: this.model.name(),
            Email: this.model.email(),
            Message: this.model.message(),
            UserAgent: navigator.userAgent,
            StudioVersion: this.studioVersion,
            ServerVersion: this.serverVersion
        }
    }

    send() {
        if (this.isValid(this.model.validationGroup)) {
            this.spinners.send(true);
            const dto = this.toDto();

            new sendFeedbackCommand(dto)
                .execute()
                .done(() => dialog.close(this, null))
                .always(() => this.spinners.send(false));
        }
    }
}

export = feedback;

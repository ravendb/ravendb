import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import sendFeedbackCommand = require("commands/resources/sendFeedbackCommand");
import dialog = require("plugins/dialog");
import router = require("plugins/router");
import studioSettings = require("common/settings/studioSettings");
import globalSettings = require("common/settings/globalSettings");

type featureImpression = 'positive' | 'negative';

class feedbackModel {
    name = ko.observable<string>();
    email = ko.observable<string>();
    message = ko.observable<string>();
    viewSpecific = ko.observable<boolean>(false);
    featureImpression = ko.observable<featureImpression>();

    validationGroup = ko.validatedObservable({
        name: this.name,
        email: this.email,
        message: this.message
    });

    constructor() {
        this.setupValidation();
    }

    private setupValidation() {
        this.name.extend({
            required: true
        });

        this.email.extend({
            required: true,
            email: true
        });

        this.message.extend({
            required: {
                onlyIf: () => !this.viewSpecific() || this.featureImpression() === 'negative'
            }
        });
    }
}

class feedback extends dialogViewModelBase {

    view = require("views/shell/feedback.html");

    private readonly studioVersion: string;
    private readonly serverVersion: string;
    private moduleTitle = ko.observable<string>();
    private moduleId = ko.observable<string>();
    private globalSettings: globalSettings;

    model = new feedbackModel();

    spinners = {
        send: ko.observable<boolean>(false)
    };

    constructor(studioVersion: string, serverVersion: string) {
        super();
        this.studioVersion = studioVersion;
        this.serverVersion = serverVersion;

        const instruction = router.activeInstruction();
        if (instruction) {
            this.moduleTitle(instruction.config.title);
            this.moduleId((instruction.config.moduleId as Function)?.name);
        }

        studioSettings.default.globalSettings()
            .done(settings => this.globalSettings = settings);
    }

    attached() {
        const feedback = this.globalSettings.feedback.getValue();
        if (feedback) {
            this.model.email(feedback.Email);
            this.model.name(feedback.Name);
        }
    }

    private toDto(): Raven.Server.Documents.Studio.FeedbackForm {
        return {
            Message: this.model.message(),
            Product: {
                FeatureImpression: this.model.viewSpecific() ? this.model.featureImpression() : null,
                FeatureName: this.model.viewSpecific() ? this.moduleTitle() : null,
                StudioView: this.moduleId(),
                StudioVersion: this.studioVersion,
                Version: this.serverVersion,
                Name: "RavenDB"
            },
            User: {
                Name: this.model.name(),
                Email: this.model.email(),
                UserAgent: navigator.userAgent
            }
        }
    }

    send() {
        if (this.isValid(this.model.validationGroup)) {
            this.spinners.send(true);
            const dto = this.toDto();

            this.globalSettings.feedback.setValue({
                Email: dto.User.Email,
                Name: dto.User.Name
            });

            new sendFeedbackCommand(dto)
                .execute()
                .done(() => dialog.close(this, null))
                .always(() => this.spinners.send(false));
        }
    }
}

export = feedback;

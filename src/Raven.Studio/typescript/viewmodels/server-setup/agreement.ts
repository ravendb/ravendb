import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import loadAgreementCommand = require("commands/setup/loadAgreementCommand");

class agreement extends setupStep {

    url = ko.observable<string>();
    
    confirmation = ko.observable<boolean>(false);
    
    validationGroup = ko.validatedObservable({
        confirmation: this.confirmation
    });
    
    constructor() {
        super();

        this.confirmation.extend({
            validation: [
                {
                    validator: (val: boolean) => val === true,
                    message: "You must accept terms & conditions"
                }
            ]
        });
    }
    
    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "LetsEncrypt" && this.model.domain().userEmail()) {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    activate(args: any) {
        super.activate(args);
        
        return new loadAgreementCommand(this.model.domain().userEmail())
            .execute()
            .done(url => {
                this.url(url);
            });
    }
    
    compositionComplete() {
        super.compositionComplete();

        const iframe = document.getElementById('terms') as HTMLIFrameElement;
        const iframedoc = iframe.contentDocument || iframe.contentWindow.document;

        iframedoc.body.innerHTML = this.prepareIFrameContent();
    }
    
    private prepareIFrameContent() {
        const template = document.getElementById("iframe-agreement-template");
        return _.replace(template.innerHTML, /{{URL}}/g, this.url());
    }
    
    save() {
        if (this.isValid(this.validationGroup)) {
            router.navigate("#nodes");
        }
    }

}

export = agreement;

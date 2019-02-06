import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import listHostsForCertificateCommand = require("commands/wizard/listHostsForCertificateCommand");
import fileImporter = require("common/fileImporter");

class certificate extends setupStep {
 
    passwordInputVisible: KnockoutComputed<boolean>;
    
    spinners = {
        hosts: ko.observable<boolean>(false)
    };
    
    constructor() {
        super();
        
        this.bindToCurrentInstance( "fileSelected");
        
        this.passwordInputVisible = ko.pureComputed(() => {
            return this.model.certificate().fileProtected();
        });        
        
        this.model.certificate().certificatePassword.extend({
            required: {
                onlyIf: () => {
                    return this.model.certificate().fileProtected()
                }
            }
        });
    }

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();
        
        this.model.domain().reusingConfiguration(false);
        
        if (mode && mode === "Secured") {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    compositionComplete() {
        super.compositionComplete();

        const certificate = this.model.certificate();
        const fetchCNsThrottled = _.debounce(() => {
            if (certificate.certificatePassword()) {
                this.fetchCNs();    
            }
        }, 300);

        
        this.registerDisposable(certificate.certificate.subscribe(() => {
            // don't throttle when new cert was supplied
            certificate.certificatePassword("");
            this.fetchCNs();
        }));
        this.registerDisposable(certificate.certificatePassword.subscribe(fetchCNsThrottled));
    }
    
    save() {
        const certs = this.model.certificate();
        
        this.tryToSetHostname();
        
        if (this.isValid(certs.validationGroup)) {
            router.navigate("#nodes");
        }
    }
    
    private tryToSetHostname() {
        // if user loaded certificate with single CN (but not wildcard)
        // then populate node info with this information
        
        const certificate = this.model.certificate();
        
        if (this.model.mode() === "Secured" && !certificate.wildcardCertificate()) {
            if (this.model.nodes().length === 1 && certificate.certificateCNs().length === 1) {
                this.model.nodes()[0].hostname(certificate.certificateCNs()[0]);
            }
        }
    }

    back() {
        router.navigate("#welcome");
    }
  
    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsDataURL(fileInput, (dataUrl: string, fileName: string) => {
            const isFileSelected = fileName ? !!fileName.trim() : false;
            this.model.certificate().certificateFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);

            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            this.model.certificate().certificate(dataUrl.substr(dataUrl.indexOf(",") + 1));
        });
    }

    private fetchCNs() {
        const cert = this.model.certificate();
        cert.certificateCNs([]);
        const password = cert.certificatePassword();
        
        this.spinners.hosts(true);
        new listHostsForCertificateCommand(cert.certificate(), password)
            .execute()
            .done((hosts: Array<string>) => {
                cert.certificateCNs(_.uniq(hosts));
                cert.certificatePassword.clearError();
                if (!password) {
                    this.model.certificate().fileProtected(false);
                }
            })
            .fail((response: JQueryXHR) => {
                if (response.status === 400) {
                    this.model.certificate().fileProtected(true);
                    cert.certificatePassword.setError("Invalid password");
                }
            })
            .always(() => this.spinners.hosts(false));
    }
}

export = certificate;

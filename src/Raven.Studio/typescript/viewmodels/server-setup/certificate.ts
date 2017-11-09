import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import listHostsForCertificateCommand = require("commands/setup/listHostsForCertificateCommand");

class certificate extends setupStep {

    certificateFileName = ko.observable<string>();
    passwordInputVisible = ko.observable<boolean>(false);
    
    constructor() {
        super();
        
        this.bindToCurrentInstance( "fileSelected");
    }

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

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
        
        if (this.isValid(certs.validationGroup)) {
            router.navigate("#nodes");
        }
    }

    back() {
        router.navigate("#welcome");
    }
  
    fileSelected(fileInput: HTMLInputElement) {
        if (fileInput.files.length === 0) {
            return;
        }

        const fileName = fileInput.value;
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.certificateFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = () => {
            const dataUrl = reader.result;
            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            this.model.certificate().certificate(dataUrl.substr(dataUrl.indexOf(",") + 1));
        };
        reader.onerror = function(error: any) {
            alert(error);
        };
        reader.readAsDataURL(file);
    }

    private fetchCNs() {
        const cert = this.model.certificate();
        cert.certificateCNs([]);
        const password = cert.certificatePassword();
        
        new listHostsForCertificateCommand(cert.certificate(), password)
            .execute()
            .done((hosts: Array<string>) => {
                cert.certificateCNs(_.uniq(hosts));
                
                if (!password) {
                    this.passwordInputVisible(false);
                }
            })
            .fail((response: JQueryXHR) => {
                if (response.status === 400) {
                    this.passwordInputVisible(true);
                }
            });
    }
}

export = certificate;

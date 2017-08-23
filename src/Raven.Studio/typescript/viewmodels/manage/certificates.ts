import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import databasesManager = require("common/shell/databasesManager");
import certificateModel = require("models/auth/certificateModel");
import getCertificatesCommand = require("commands/auth/getCertificatesCommand");
import certificatePermissionModel = require("models/auth/certificatePermissionModel");
import uploadCertificateCommand = require("commands/auth/uploadCertificateCommand");
import deleteCertificateCommand = require("commands/auth/deleteCertificateCommand");
import updateCertificatePermissionsCommand = require("commands/auth/updateCertificatePermissionsCommand");
import appUrl = require("common/appUrl");
import endpoints = require("endpoints");
import copyToClipboard = require("common/copyToClipboard");

class certificates extends viewModelBase {

    spinners = {
        processing: ko.observable<boolean>(false)
    };
    
    model = ko.observable<certificateModel>();
    showDatabasesSelector: KnockoutComputed<boolean>;
    hasAllDatabasesAccess: KnockoutComputed<boolean>;
    certificates = ko.observableArray<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition>();

    importedFileName = ko.observable<string>();
    
    newPermissionDatabaseName = ko.observable<string>();
    
    newPermissionValidationGroup: KnockoutValidationGroup = ko.validatedObservable({
        newPermissionDatabaseName: this.newPermissionDatabaseName
    });

    generateCertificateUrl = endpoints.global.adminCertificates.adminCertificates;
    generateCertPayload = ko.observable<string>();

    clearanceLabelFor = certificateModel.clearanceLabelFor;
    
    constructor() {
        super();

        this.bindToCurrentInstance("onCloseEdit", "save", "enterEditCertificateMode", 
            "deletePermission", "addNewPermission", "fileSelected", "copyThumbprint",
            "useDatabase", "deleteCertificate");
        this.initObservables();
        this.initValidation();
    }
    
    activate() {
        return this.loadCertificates();
    }
    
    
    private initObservables() {
        this.showDatabasesSelector = ko.pureComputed(() => {
            if (!this.model()) {
                return false;
            }
            
            return this.model().securityClearance() === "ValidUser";
        });
    }
    
    private initValidation() {
        this.newPermissionDatabaseName.extend({
            required: true
        });
    }
    
    enterEditCertificateMode(itemToEdit: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) {
        this.model(certificateModel.fromDto(itemToEdit));
        this.model().validationGroup.errors.showAllMessages(false);
    }

    deleteCertificate(certificate: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) {
        this.confirmationMessage("Are you sure?", "Do you want to delete certificate with thumbprint: " + certificate.Thumbprint + "", ["No", "Yes, delete"])
            .done(result => {
                //TODO: spinners
                if (result.can) {
                    new deleteCertificateCommand(certificate.Thumbprint)
                        .execute()
                        .always(() => this.loadCertificates());
                }
            });
    }
    
    enterGenerateCertificateMode() {
        this.model(certificateModel.generate());
    }
    
    enterUploadCertificateMode() {
        this.model(certificateModel.upload());
    }

    fileSelected(fileInput: HTMLInputElement) {
        if (fileInput.files.length === 0) {
            return;
        }
        
        const fileName = fileInput.value;
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
        
        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = () => {
            const dataUrl = reader.result;
            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            this.model().certificateAsBase64(dataUrl.substr(dataUrl.indexOf(",") + 1));
        };
        reader.onerror = function(error: any) {
            alert(error);
        };
        reader.readAsDataURL(file);
    }

    save() {
        this.newPermissionValidationGroup.errors.showAllMessages(false);
        
        if (!this.isValid(this.model().validationGroup)) {
            return;
        }
        
        this.spinners.processing(true);
        
        const model = this.model();
        switch (model.mode()) {
            case "generate":
                this.generateCertPayload(JSON.stringify(model.toGenerateCertificateDto()));
                
                $("form[target=certificate_download_iframe]").submit();
                
                setTimeout(() => {
                    this.spinners.processing(false);
                    this.loadCertificates();
                    this.onCloseEdit();
                }, 3000);
                
                break;
            case "upload":
                new uploadCertificateCommand(model)
                    .execute()
                    .always(() => {
                        this.spinners.processing(false);
                        this.loadCertificates();
                        this.onCloseEdit();
                    });
                break;
                
            case "editExisting":
                new updateCertificatePermissionsCommand(model)
                    .execute()
                    .always(() => {
                        this.spinners.processing(false);
                        this.loadCertificates();
                        this.onCloseEdit();
                    });
                break;
        }
    }
    
    private loadCertificates() {
        return new getCertificatesCommand()
            .execute()
            .done(certificates => {
                this.certificates(certificates);
            });
    }
    
    onCloseEdit() {
        this.model(null);
    }
    
    deletePermission(permission: certificatePermissionModel) {
        const model = this.model();
        model.permissions.remove(permission);
    }

    useDatabase(databaseName: string) {
        this.newPermissionDatabaseName(databaseName);
        this.addNewPermission();
    }
    
    addNewPermission() {
        if (!this.isValid(this.newPermissionValidationGroup)) {
            return;
        }
        
        const permission = new certificatePermissionModel();
        permission.databaseName(this.newPermissionDatabaseName());
        permission.accessLevel("ReadWrite");
        this.model().permissions.push(permission);
        this.newPermissionDatabaseName("");
        this.newPermissionValidationGroup.errors.showAllMessages(false);
    }

    createDatabaseNameAutocompleter() {
        return ko.pureComputed(() => {
            const key = this.newPermissionDatabaseName();
            
            const existingPermissions = this.model().permissions().map(x => x.databaseName());
            
            const dbNames = databasesManager.default.databases()
                .map(x => x.name)
                .filter(x => !_.includes(existingPermissions, x));

            if (key) {
                return dbNames.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return dbNames;
            }
        });
    }
    
    resolveDatabasesAccess(certificateDefinition: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition): Array<string> {
        if (certificateDefinition.SecurityClearance === "ClusterAdmin" || certificateDefinition.SecurityClearance === "Operator") {
            return ["All"];
        }
        return Object.keys(certificateDefinition.Permissions);
    }

    copyThumbprint(model: certificateModel) {
        copyToClipboard.copy(model.thumbprint(), "Thumbprint was copied to clipboard.");
    }
}

export = certificates;

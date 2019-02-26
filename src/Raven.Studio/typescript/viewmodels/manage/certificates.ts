import viewModelBase = require("viewmodels/viewModelBase");
import databasesManager = require("common/shell/databasesManager");
import certificateModel = require("models/auth/certificateModel");
import getCertificatesCommand = require("commands/auth/getCertificatesCommand");
import certificatePermissionModel = require("models/auth/certificatePermissionModel");
import uploadCertificateCommand = require("commands/auth/uploadCertificateCommand");
import deleteCertificateCommand = require("commands/auth/deleteCertificateCommand");
import replaceClusterCertificateCommand = require("commands/auth/replaceClusterCertificateCommand");
import updateCertificatePermissionsCommand = require("commands/auth/updateCertificatePermissionsCommand");
import getServerCertificateSetupModeCommand = require("commands/auth/getServerCertificateSetupModeCommand");
import forceRenewServerCertificateCommand = require("commands/auth/forceRenewServerCertificateCommand");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import notificationCenter = require("common/notifications/notificationCenter");
import getClusterDomainsCommand = require("commands/auth/getClusterDomainsCommand");
import endpoints = require("endpoints");
import copyToClipboard = require("common/copyToClipboard");
import popoverUtils = require("common/popoverUtils");
import messagePublisher = require("common/messagePublisher");
import eventsCollector = require("common/eventsCollector");
import changesContext = require("common/changesContext");
import accessManager = require("common/shell/accessManager");
import getServerCertificateRenewalDateCommand = require("commands/auth/getServerCertificateRenewalDateCommand");
import fileImporter = require("common/fileImporter");

interface unifiedCertificateDefinitionWithCache extends unifiedCertificateDefinition {
    expirationClass: string;
    expirationText: string;
    expirationIcon: string;
}

class certificates extends viewModelBase {

    spinners = {
        processing: ko.observable<boolean>(false)
    };
    
    nameFilter = ko.observable<string>("");
    clearanceFilter = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>();
    
    model = ko.observable<certificateModel>();
    showDatabasesSelector: KnockoutComputed<boolean>;
    canExportClusterCertificates: KnockoutComputed<boolean>;
    canReplaceClusterCertificate: KnockoutComputed<boolean>;
    certificates = ko.observableArray<unifiedCertificateDefinition>();
    serverCertificateThumbprint = ko.observable<string>();
    serverCertificateSetupMode = ko.observable<Raven.Server.Commercial.SetupMode>();
    serverCertificateRenewalDate = ko.observable<string>();
    wellKnownAdminCerts = ko.observableArray<string>([]);
    wellKnownAdminCertsVisible = ko.observable<boolean>(false);
    
    domainsForServerCertificate = ko.observableArray<string>([]);
    
    usingHttps = location.protocol === "https:";
    
    resolveDatabasesAccess = certificateModel.resolveDatabasesAccess;
    accessManager = accessManager.default.certificatesView;

    importedFileName = ko.observable<string>();
    
    newPermissionDatabaseName = ko.observable<string>();
    
    newPermissionValidationGroup: KnockoutValidationGroup = ko.validatedObservable({
        newPermissionDatabaseName: this.newPermissionDatabaseName
    });

    generateCertificateUrl = endpoints.global.adminCertificates.adminCertificates;
    exportCertificateUrl = endpoints.global.adminCertificates.adminCertificatesExport;
    generateCertPayload = ko.observable<string>();

    clearanceLabelFor = certificateModel.clearanceLabelFor;
    securityClearanceTypes = certificateModel.securityClearanceTypes;
    
    constructor() {
        super();

        this.bindToCurrentInstance("onCloseEdit", "save", "enterEditCertificateMode", 
            "deletePermission", "addNewPermission", "fileSelected", "copyThumbprint",
            "useDatabase", "deleteCertificate", "renewServerCertificate", "canBeAutomaticallyRenewed");
        this.initObservables();
        this.initValidation();
        
        this.nameFilter.throttle(300).subscribe(() => this.filterCertificates());
        this.clearanceFilter.subscribe(() => this.filterCertificates());
    }
    
    activate() {
        this.loadCertificates();
        
        if (accessManager.default.certificatesView.canRenewLetsEncryptCertificate()) {
            new getServerCertificateSetupModeCommand()
                .execute()
                .done((setupMode: Raven.Server.Commercial.SetupMode) => {
                    this.serverCertificateSetupMode(setupMode);

                    if (setupMode === "LetsEncrypt") {
                        this.fetchRenewalDate();
                    }
                });
        }
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.addNotification(changesContext.default.serverNotifications().watchAllAlerts(alert => this.onAlert(alert)));

        $(".js-export-certificates").tooltip({
            container: "body",
            placement: "right"
        });
        
        this.model.subscribe(model  => {
            if (model) {
                this.initPopover();
            }
        });
    }
    
    private filterCertificates() {
        const filter = this.nameFilter().toLocaleLowerCase();
        const clearance = this.clearanceFilter();
        
        this.certificates().forEach(certificate => {
            const nameMatch = !certificate || certificate.Name.toLocaleLowerCase().includes(filter);
            const clearanceMatch = !clearance || certificate.SecurityClearance === clearance;
            const thumbprintMatch = !certificate || _.some(certificate.Thumbprints, x => x.toLocaleLowerCase().includes(filter));
            certificate.Visible((nameMatch || thumbprintMatch) && clearanceMatch);
        });
        
        const wellKnownAdminCerts = this.wellKnownAdminCerts();
        
        if (wellKnownAdminCerts.length) {
            const clearanceMatch = !clearance || clearance === "ClusterAdmin";
            const thumbprintMatch = _.some(wellKnownAdminCerts, x => x.toLocaleLowerCase().includes(filter));
            this.wellKnownAdminCertsVisible(thumbprintMatch && clearanceMatch);
        } else {
            this.wellKnownAdminCertsVisible(false);
        }
    }
    
    private onAlert(alert: Raven.Server.NotificationCenter.Notifications.AlertRaised) {
        if (alert.AlertType === "Certificates_ReplaceError" ||
            alert.AlertType === "Certificates_ReplaceSuccess" ||
            alert.AlertType === "Certificates_EntireClusterReplaceSuccess") {
            this.loadCertificates();
        }
    }
    
    private initPopover() {
        popoverUtils.longWithHover($(".certificate-file-label small"),
            {
                content: () => {
                    switch (this.model().mode()) {
                        case "replace":
                            return 'Certificate file cannot be password protected.';
                        case "upload":
                            return 'Select .pfx store file with single or multiple certificates. All of them will be imported under a single name.';
                    }
                },
                placement: "top"
            });
    }
    
    private initObservables() {
        this.showDatabasesSelector = ko.pureComputed(() => {
            if (!this.model()) {
                return false;
            }
            
            return this.model().securityClearance() === "ValidUser";
        });
        
        this.canExportClusterCertificates = ko.pureComputed(() => {
            const certs = this.certificates();
            return _.some(certs, x => x.SecurityClearance === "ClusterNode");
        });
        
        this.canReplaceClusterCertificate = ko.pureComputed(() => {
            const certs = this.certificates();
            return _.some(certs, x => x.SecurityClearance === "ClusterNode");
        });
    }
    
    private initValidation() {
        this.newPermissionDatabaseName.extend({
            required: true
        });
    }    
    
    private fetchRenewalDate() {
        return new getServerCertificateRenewalDateCommand()
            .execute()
            .done(dateAsString => {
                const date = moment.utc(dateAsString);
                const dateFormatted = date.format("YYYY-MM-DD");
                this.serverCertificateRenewalDate(dateFormatted);
            })
    }
    
    canBeAutomaticallyRenewed(thumbprints: string[]) {
        return ko.pureComputed(() => {
            return _.includes(thumbprints, this.serverCertificateThumbprint()) && this.serverCertificateSetupMode() === 'LetsEncrypt';
        });
    }
    
    renewServerCertificate() {
        this.confirmationMessage("Server certificate renewal", "Do you want to renew the server certificate?", ["No", "Yes, renew"])
            .done(result => {
                if (result.can) {
                    new forceRenewServerCertificateCommand()
                        .execute();
                }
            });
    }
    
    enterEditCertificateMode(itemToEdit: unifiedCertificateDefinition) {
        this.model(certificateModel.fromDto(itemToEdit));
        this.model().validationGroup.errors.showAllMessages(false);
    }

    deleteCertificate(certificate: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) {
        this.confirmationMessage("Are you sure?", "Do you want to delete certificate with thumbprint: " + certificate.Thumbprint + "", ["No", "Yes, delete"])
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("certificates", "delete");
                    new deleteCertificateCommand(certificate.Thumbprint)
                        .execute()
                        .always(() => this.loadCertificates());
                }
            });
    }

    exportClusterCertificates() {
        eventsCollector.default.reportEvent("certificates", "export-certs");
        const targetFrame = $("form#certificates_export_form");
        targetFrame.attr("action", this.exportCertificateUrl);
        targetFrame.submit();
    }
    
    enterGenerateCertificateMode() {
        eventsCollector.default.reportEvent("certificates", "generate");
        this.model(certificateModel.generate());
    }
    
    enterUploadCertificateMode() {
        eventsCollector.default.reportEvent("certificates", "upload");
        this.model(certificateModel.upload());
    }
    
    replaceClusterCertificate() {
        eventsCollector.default.reportEvent("certificates", "replace");
        this.model(certificateModel.replace());
        
        new getClusterDomainsCommand()
            .execute()
            .done(domains => this.domainsForServerCertificate(domains));
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsDataURL(fileInput, (dataUrl, fileName) => {
            const isFileSelected = fileName ? !!fileName.trim() : false;
            this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);

            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            this.model().certificateAsBase64(dataUrl.substr(dataUrl.indexOf(",") + 1));
        });
    }

    save() {
        this.newPermissionValidationGroup.errors.showAllMessages(false);

        const model = this.model();
        
        if (!this.isValid(model.validationGroup)) {
            return;
        }

        const maybeWarnTask = $.Deferred<void>();
        
        if (this.model().mode() !== "replace" && model.securityClearance() === "ValidUser" && model.permissions().length === 0) {
            this.confirmationMessage("Did you forget about assigning database privileges?",
            "Leaving the database privileges section empty is going to prevent users from accessing the database.",
            ["I want to assign privileges", "Save anyway"],
                true)
            .done(result => {
                if (result.can) {
                    maybeWarnTask.resolve();
                }
            });
        } else {
            maybeWarnTask.resolve();
        }

        maybeWarnTask
            .done(() => {
                this.spinners.processing(true);

                switch (model.mode()) {
                    case "generate":
                        this.generateCertPayload(JSON.stringify(model.toGenerateCertificateDto()));

                        new getNextOperationId(null)
                            .execute()
                            .done(operationId => {

                                const targetFrame = $("form#certificate_download_form");
                                targetFrame.attr("action", this.generateCertificateUrl + "?operationId=" + operationId);
                                targetFrame.submit();

                                notificationCenter.instance.monitorOperation(null, operationId)
                                    .done(() => {
                                        messagePublisher.reportSuccess("Client certificate was generated.");
                                    })
                                    .fail(() => {
                                        notificationCenter.instance.openDetailsForOperationById(null, operationId);
                                    })
                                    .always(() => {
                                        this.spinners.processing(false);
                                        this.onCloseEdit();
                                        this.loadCertificates();
                                    })
                            })
                            .fail(() => {
                                this.spinners.processing(false);
                                this.onCloseEdit();
                            });
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
                    case "replace":
                        new replaceClusterCertificateCommand(model)
                            .execute()
                            .always(() => {
                                this.spinners.processing(false);
                                this.loadCertificates();
                                this.onCloseEdit();
                            });
                }
            });
    }
    
    private loadCertificates() {
        return new getCertificatesCommand(true)
            .execute()
            .done(certificatesInfo => {
                let mergedCertificates = [] as Array<unifiedCertificateDefinition>;
                
                const secondaryCertificates = [] as Array<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition>;
                
                certificatesInfo.Certificates.forEach(cert => {
                    if (cert.CollectionPrimaryKey) {
                        secondaryCertificates.push(cert);
                    } else {
                        (cert as unifiedCertificateDefinition).Thumbprints = [cert.Thumbprint];
                        (cert as unifiedCertificateDefinition).Visible = ko.observable<boolean>(true);
                        mergedCertificates.push(cert as unifiedCertificateDefinition);
                    }
                });
                
                this.serverCertificateThumbprint(certificatesInfo.LoadedServerCert);
                
                secondaryCertificates.forEach(cert => {
                    const thumbprint = cert.CollectionPrimaryKey.split("/")[1];
                    const primaryCert = mergedCertificates.find(x => x.Thumbprint === thumbprint);
                    primaryCert.Thumbprints.push(cert.Thumbprint);
                });
                
                mergedCertificates = _.sortBy(mergedCertificates, x => x.Name.toLocaleLowerCase());
                this.updateCache(mergedCertificates);
                this.certificates(mergedCertificates);
                this.wellKnownAdminCerts(certificatesInfo.WellKnownAdminCerts || []);
                this.filterCertificates();
            });
    }
    
    private updateCache(certificates: Array<unifiedCertificateDefinition>) {
        certificates.forEach((cert: unifiedCertificateDefinitionWithCache) => {
            const date = moment.utc(cert.NotAfter);
            const dateFormatted = date.format("YYYY-MM-DD");
            
            const nowPlusMonth = moment.utc().add(1, 'months');
            
            if (date.isBefore()) {
                cert.expirationText = 'Expired ' + dateFormatted;
                cert.expirationIcon = "icon-danger";
                cert.expirationClass = "text-danger"
            } else if (date.isAfter(nowPlusMonth)) {
                cert.expirationText = dateFormatted;
                cert.expirationIcon =  "icon-clock";
                cert.expirationClass = "";
            } else {
                cert.expirationText = dateFormatted;
                cert.expirationIcon =  "icon-warning";
                cert.expirationClass = "text-warning";
            }
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
    
    copyThumbprint(thumbprint: string) {
        copyToClipboard.copy(thumbprint, "Thumbprint was copied to clipboard.");
    }
    
    canDelete(securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance) {
        return ko.pureComputed(() => {
            if (!this.accessManager.canDeleteClusterAdminCertificate() && securityClearance === "ClusterAdmin") {
                return false;
            }
            
            if (!this.accessManager.canDeleteClusterNodeCertificate() && securityClearance === "ClusterNode") {
                return false;
            }
            
            return true; 
        });
    }

    canGenerateCertificateForSecurityClearanceType(securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance) {
        return ko.pureComputed(() => {
            if (!this.accessManager.canGenerateClientCertificateForAdmin() && securityClearance === "ClusterAdmin") {
                return false;
            }

            return true;
        });
    }
}

export = certificates;

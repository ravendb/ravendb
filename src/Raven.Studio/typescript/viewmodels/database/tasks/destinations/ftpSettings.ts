import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");
import fileImporter = require("common/fileImporter");
import genUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");

class ftpSettings extends backupSettings {
    view = require("views/database/tasks/destinations/ftpSettings.html");
    
    url = ko.observable<string>();
    userName = ko.observable<string>();
    password = ko.observable<string>();
    isPasswordHidden = ko.observable<boolean>(true);
    certificateAsBase64 = ko.observable<string>();

    isLoadingFile = ko.observable<boolean>();
    isFtps = ko.pureComputed(() => {
        if (!this.url())
            return false;

        return this.url().toLowerCase().startsWith("ftps://");
    });

    targetOperation: string;

    constructor(dto: Raven.Client.Documents.Operations.Backups.FtpSettings, targetOperation: string) {
        super(dto, "FTP");

        this.url(dto.Url || "");
        this.userName(dto.UserName);
        this.password(dto.Password);
        this.certificateAsBase64(dto.CertificateAsBase64);
        
        this.targetOperation = targetOperation;

        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.url,
            this.userName,
            this.password,
            this.certificateAsBase64,
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    compositionComplete() {
        popoverUtils.longWithHover($(".ftp-host-info"),
            {
                content: tasksCommonContent.ftpHostInfo
            });
    }

    initValidation() {
        this.url.extend({
            required: {
                onlyIf: () => this.enabled()
            },
            validation: [
                {
                    validator: (url: string) => {
                        if (!url)
                            return false;

                        const urlLower = url.toLowerCase();
                        if (urlLower.includes("://") && !urlLower.startsWith("ftp://") && !urlLower.startsWith("ftps://")) {
                            return false;
                        }

                        return true;
                    },
                    message: "Url must start with ftp:// or ftps://"
                }
            ]
        });

        this.userName.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.password.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.certificateAsBase64.extend({
            required: {
                onlyIf: () => this.enabled() && this.isFtps()
            }
        });

        this.localConfigValidationGroup = ko.validatedObservable({
            url: this.url,
            userName: this.userName,
            password: this.password,
            certificateAsBase64: this.certificateAsBase64
        });
    }

    fileSelected(fileInput: HTMLInputElement) {
        this.isLoadingFile(true);
        
        fileImporter.readAsArrayBuffer(fileInput, (data) => {
            let binary = "";
            const bytes = new Uint8Array(data);
            for (let i = 0; i < bytes.byteLength; i++) {
                binary += String.fromCharCode(bytes[i]);
            }
            const result = window.btoa(binary);
            this.certificateAsBase64(result);
        })
            .always(() => this.isLoadingFile(false));
    }

    toDto(): Raven.Client.Documents.Operations.Backups.FtpSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.FtpSettings;
        dto.Url = this.url();
        dto.UserName = this.userName();
        dto.Password = this.password();
        dto.CertificateAsBase64 = this.isFtps() ? this.certificateAsBase64() : null;

        return genUtils.trimProperties(dto, ["Url", "UserName"]);
    }

    static empty(targetOperation: string): ftpSettings {
        return new ftpSettings({
            Disabled: true,
            Url: null,
            UserName: null,
            Password: null,
            CertificateAsBase64: null,
            GetBackupConfigurationScript: null
        }, targetOperation);
    }

    toggleIsPasswordHidden() {
        this.isPasswordHidden(!this.isPasswordHidden());
    }
}

export = ftpSettings;

import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");

class ftpSettings extends backupSettings {
    url = ko.observable<string>();
    port = ko.observable<number>();
    userName = ko.observable<string>();
    password = ko.observable<string>();
    certificateAsBase64 = ko.observable<string>();
    certificateFileName = ko.observable<string>();

    isLoadingFile = ko.observable<boolean>();
    isFtps = ko.pureComputed(() => {
        if (!this.url())
            return false;

        return this.url().toLowerCase().startsWith("ftps://");
    });

    constructor(dto: Raven.Client.ServerWide.PeriodicBackup.FtpSettings) {
        super(dto);

        this.url(dto.Url);
        this.port(dto.Port);
        this.userName(dto.UserName);
        this.password(dto.Password);
        this.certificateAsBase64(dto.CertificateAsBase64);
        this.certificateFileName(dto.CertificateFileName);

        if (this.certificateAsBase64() && !this.certificateFileName()) {
            // the configuration was updated using the client api
            this.certificateFileName("certificate.cer");
        }

        this.connectionType = "FTP";
        this.initValidation();
    }

    initValidation() {
        this.url.extend({
            required: {
                onlyIf: () => this.enabled()
            },
            validation: [
                {
                    validator: (url: string) => this.validate(() => {
                        if (!url)
                            return false;

                        const urlLower = url.toLowerCase();
                        if (urlLower.includes("://") && !urlLower.startsWith("ftp://") && !urlLower.startsWith("ftps://")) {
                            return false;
                        }

                        return true;
                    }),
                    message: "Url must start with ftp:// or ftps://"
                }
            ]
        });

        this.port.extend({
            validation: [
                {
                    validator: (port: number) => this.validate(() => {
                        if (!this.enabled())
                            return true;

                        if (!port)
                            return true;

                        return port >= 1 && port <= 65535;
                    }),
                    message: "Port number range: 1-65535"
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

        this.certificateFileName.extend({
            required: {
                onlyIf: () => this.enabled() && this.isFtps()
            }
        });

        this.validationGroup = ko.validatedObservable({
            url: this.url,
            port: this.port,
            userName: this.userName,
            password: this.password,
            certificateFileName: this.certificateFileName
        });
    }

    fileSelected() {
        this.isLoadingFile(true);
        const fileInput = document.querySelector("#filePicker") as HTMLInputElement;
        const fileList = fileInput.files;
        const fileReader = new FileReader();
        if (!(fileReader && fileList && fileList.length)) {
            this.isLoadingFile(false);
            return;
        }

        const file = fileList[0];
        this.certificateFileName(file.name);

        fileReader.readAsArrayBuffer(file);
        fileReader.onload = () => {
            let binary = "";
            const bytes = new Uint8Array(fileReader.result);
            for (let i = 0; i < bytes.byteLength; i++) {
                binary += String.fromCharCode(bytes[i]);
            }
            const result = window.btoa(binary);
            this.certificateAsBase64(result);
            this.isLoadingFile(false);
        };
        fileReader.onerror = () => this.isLoadingFile(false);
    }

    toDto(): Raven.Client.ServerWide.PeriodicBackup.FtpSettings {
        const dto = super.toDto() as Raven.Client.ServerWide.PeriodicBackup.FtpSettings;
        dto.Url = this.url();
        dto.Port = this.port();
        dto.UserName = this.userName();
        dto.Password = this.password();
        dto.CertificateAsBase64 = this.isFtps() ? this.certificateAsBase64() : null;
        dto.CertificateFileName = this.isFtps() ? this.certificateFileName() : null;
        return dto;
    }

    static empty(): ftpSettings {
        return new ftpSettings({
            Disabled: true,
            Url: null,
            Port: null,
            UserName: null,
            Password: null,
            CertificateAsBase64: null,
            CertificateFileName: null
        });
    }
}

export = ftpSettings;

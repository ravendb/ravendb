import generateSecretCommand = require("commands/database/secrets/generateSecretCommand");
import distributeSecretCommand = require("commands/database/secrets/distributeSecretCommand");
import copyToClipboard = require("common/copyToClipboard");
import fileDownloader = require("common/fileDownloader");

abstract class setupEncryptionKey {
    
    protected readonly key: KnockoutObservable<string>;
    private readonly keyConfirmation: KnockoutObservable<boolean>;
    protected readonly databaseName: KnockoutObservable<string>;

    canProvideOwnKey = ko.observable<boolean>(true);

    disableSavingKeyData: KnockoutComputed<boolean>;
    saveKeyValidationGroup: KnockoutValidationGroup;
    
    isKeyAvailableAgain = ko.observable<boolean>(false);
    
    protected constructor(key: KnockoutObservable<string>, keyConfirmation: KnockoutObservable<boolean>, databaseName: KnockoutObservable<string>, isKeyAvailableAgain: boolean = false) {
        this.key = key;
        this.keyConfirmation = keyConfirmation;
        this.databaseName = databaseName;
        this.isKeyAvailableAgain(isKeyAvailableAgain);
        
        this.saveKeyValidationGroup = ko.validatedObservable({
            name: this.databaseName,
            key: this.key
        });

        this.disableSavingKeyData = ko.pureComputed(() => {
            return !this.databaseName() || !this.saveKeyValidationGroup.isValid();
        });
    }
    
    // currently displayed QR Code
    private qrCode: any;

    configureEncryption(encryptionKey: string, nodeTags: Array<string>): JQueryPromise<void> {
        return new distributeSecretCommand(this.databaseName(), encryptionKey, nodeTags)
            .execute();
    }
    
    generateEncryptionKey(): JQueryPromise<string> {
        return new generateSecretCommand()
            .execute()
            .done(secret => this.key(secret));
    }
    
    abstract getContainer(): HTMLElement;
    
    abstract getFileName(): string;
    
    syncQrCode() {
        const key = this.key();
        const qrContainer = document.getElementById("encryption_qrcode");
        
        if (qrContainer.innerHTML && !this.qrCode) {
            // clean up old instances
            qrContainer.innerHTML = "";
        }
        
        const isKeyValid = this.key.isValid();

        if (isKeyValid) {
            if (!this.qrCode) {
                this.qrCode = new QRCode(qrContainer, {
                    text: key,
                    width: 256,
                    height: 256,
                    colorDark: "#000000",
                    colorLight: "#ffffff",
                    correctLevel: QRCode.CorrectLevel.Q
                });
            } else {
                this.qrCode.clear();
                this.qrCode.makeCode(key);
            }
        } else {
            if (this.qrCode) {
                this.qrCode.clear();
            }
        }
    }

    abstract keyDataText(): string;
    
    copyEncryptionKeyToClipboard() {
        const container = this.getContainer();
        copyToClipboard.copy(this.keyDataText(), "Encryption key data was copied to clipboard", container);
    }

    downloadEncryptionKey() {
        //TODO: content based on context
        const text = this.keyDataText();
        const textFileName = this.getFileName();
        fileDownloader.downloadAsTxt(text, textFileName);
    }

    printEncryptionKey() {
        const text = this.keyDataText().replace(/\r\n/g, "<br/>");
        const qrCodeHtml = document.getElementById("encryption_qrcode").innerHTML;
        const docTitle = this.getFileName();

        const html = `
            <html>
                <head>
                    <title>${docTitle}</title>
                    <style>
                        body {
                            text-align: center;
                            font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
                        }
                        #encryption_qrcode {
                            position: relative;
                            display: inline-block;
                        }

                        h4 {
                            font-weight: normal;
                        }
                    
                        .qr_logo {
                            position: absolute;
                            left: 50%;
                            top: 50%;
                            -moz-transform: translateX(-50%) translateY(-50%);
                            -webkit-transform: translateX(-50%) translateY(-50%);
                            -o-transform: translateX(-50%) translateY(-50%);
                            -ms-transform: translateX(-50%) translateY(-50%);
                            transform: translateX(-50%) translateY(-50%);
                        }
                    </style>
                </head>
                <body>
                    <h4>${text}</h4>
                    <br />
                    <div id="encryption_qrcode">
                        <img class="qr_logo" src="data:image/png;base64,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">
                        ${qrCodeHtml}
                    </div>
                </body>                
            </html>
        `;

        const printWindow = window.open();
        printWindow.document.write(html);
        printWindow.document.close();

        printWindow.focus();
        setTimeout(() => {
            printWindow.print();
            printWindow.close();
        }, 50);
    }
    
    static setupKeyValidation(key: KnockoutObservable<string>) {
        key.extend({
            required: true,
            base64: true
        });
    }
    
    static setupConfirmationValidation(confirmation: KnockoutObservable<boolean>, isRequired: KnockoutObservable<boolean> | boolean = true) {
        confirmation.extend({
            validation: [
                {
                    validator: (v: boolean) => {
                        const required = ko.unwrap(isRequired);
                        return !required || v;
                    },
                    message: "Please confirm that you have saved the encryption key"
                }
            ]
        });
    }
    
    static forDatabase(key: KnockoutObservable<string>, keyConfirmation: KnockoutObservable<boolean>, databaseName: KnockoutObservable<string>) {
        return new databaseSetupEncryptionKey(key, keyConfirmation, databaseName);
    }
    
    static forBackup(key: KnockoutObservable<string>, keyConfirmation: KnockoutObservable<boolean>, databaseName: KnockoutObservable<string>) {
        return new backupSetupEncryptionKey(key, keyConfirmation, databaseName, true);
    }

    static forServerWideBackup(key: KnockoutObservable<string>, keyConfirmation: KnockoutObservable<boolean>) {
        return new serverWideBackupSetupEncryptionKey(key, keyConfirmation);
    }

    static forExport(key: KnockoutObservable<string>, keyConfirmation: KnockoutObservable<boolean>, databaseName: KnockoutObservable<string>) {
        return new exportSetupEncryptionKey(key, keyConfirmation, databaseName);
    }
}

class databaseSetupEncryptionKey extends setupEncryptionKey {
    getContainer() {
        return document.getElementById("encryption_qrcode");
    }
    
    getFileName() {
        return `Key-of-${this.databaseName()}-${moment().format("YYYY-MM-DD-HH-mm")}.txt`;
    }

    keyDataText(): string {
        const encryptionKey = this.key();
        const databaseName = this.databaseName();
        return `Encryption Key for database '${databaseName}': ${encryptionKey}\r\n\r\nThis key is used to encrypt the RavenDB database, it is required for restoring the database.\r\nMake sure you keep it in a private, safe place as it will Not be available again !`;
    }
}

class backupSetupEncryptionKey extends setupEncryptionKey {
    getContainer() {
        return document.getElementsByTagName("body")[0];
    }

    getFileName() {
        return `Backup-key-of-${this.databaseName()}-${moment().format("YYYY-MM-DD-HH-mm")}.txt`;
    }

    keyDataText(): string {
        const encryptionKey = this.key();
        const databaseName = this.databaseName();
        return `Backup Encryption Key for database '${databaseName}': ${encryptionKey}\r\n\r\nThis key is used to encrypt backup, it is required for restoring the database.\r\nMake sure you keep it in a private, safe place.`;
    }
}

class serverWideBackupSetupEncryptionKey extends setupEncryptionKey {
    constructor(key: KnockoutObservable<string>, keyConfirmation: KnockoutObservable<boolean>) {
        super(key, keyConfirmation, ko.observable<string>("ServerWide"), true);
        // The 3'rd param passed to super() is needed only for validation. Not actually used.
    }
    
    getContainer() {
        return document.getElementsByTagName("body")[0];
    }

    getFileName() {
        return `Encryption-key-for-Server-Wide-Backup-${moment().format("YYYY-MM-DD-HH-mm")}.txt`;
    }

    keyDataText(): string {
        const encryptionKey = this.key();
        return `Encryption Key for Server-Wide-Backup: ${encryptionKey}\r\n\r\nThis key is used to encrypt the server-wide-backup, it is required for restoring the data.\r\nMake sure you keep it in a private, safe place.`;
    }
}

class exportSetupEncryptionKey extends setupEncryptionKey {
    getContainer() {
        return document.getElementsByTagName("body")[0];
    }

    getFileName() {
        return `Export-key-of-${this.databaseName()}-${moment().format("YYYY-MM-DD-HH-mm")}.txt`;
    }

    keyDataText(): string {
        const encryptionKey = this.key();
        const databaseName = this.databaseName();
        return `Encryption Key for exported database '${databaseName}': ${encryptionKey}\r\n\r\nThis key is used to encrypt export, it is required for importing the database.\r\nMake sure you keep it in a private, safe place.`;
    }
}

export = setupEncryptionKey;

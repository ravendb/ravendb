import generateSecretCommand = require("commands/database/secrets/generateSecretCommand");
import distributeSecretCommand = require("commands/database/secrets/distributeSecretCommand");
import copyToClipboard = require("common/copyToClipboard");
import fileDownloader = require("common/fileDownloader");
import moment = require("moment");
import qrcodejs = require("qrcodejs");

const qrLogo = require("Content/img/qr_logo.png");

abstract class setupEncryptionKey {

    setupEncryptionKeyView = require("views/resources/setupEncryptionKey.html");
    
    protected readonly key: KnockoutObservable<string>;
    private readonly keyConfirmation: KnockoutObservable<boolean>;
    protected readonly databaseName: KnockoutObservable<string>;

    canProvideOwnKey = ko.observable<boolean>(true);

    disableSavingKeyData: KnockoutComputed<boolean>;
    saveKeyValidationGroup: KnockoutValidationGroup;
    
    isKeyAvailableAgain = ko.observable<boolean>(false);
    
    protected constructor(key: KnockoutObservable<string>, keyConfirmation: KnockoutObservable<boolean>, databaseName: KnockoutObservable<string>, isKeyAvailableAgain = false) {
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
                this.qrCode = new qrcodejs.QRCode(qrContainer, {
                    text: key,
                    width: 256,
                    height: 256,
                    colorDark: "#000000",
                    colorLight: "#ffffff",
                    correctLevel: qrcodejs.QRCode.CorrectLevel.Q
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
                        <img class="qr_logo" src=${qrLogo}>
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

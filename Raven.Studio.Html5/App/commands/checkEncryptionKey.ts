import commandBase = require("commands/commandBase");

class checkEncryptionKey extends commandBase {

    constructor(private key) {
        super();
    }

    execute() {
        var keyObject = { "key": this.key };
        var result = this.post("/studio-tasks/is-base-64-key", keyObject, null);

        if (Boolean(result) == false) {
            this.reportError("The key must be in base64 encoding format!");
        }
        return result;
    }
}

export = checkEncryptionKey; 
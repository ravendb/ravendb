import commandBase = require("commands/commandBase");

class checkEncryptionKey extends commandBase {

    constructor(private key: string) {
        super();
    }

    execute() {
        var keyObject = { "key": this.key };
        var result = this.post("/studio-tasks/is-base-64-key", keyObject, null);//TODO: use endpoints

        result.fail((response: JQueryXHR)=> this.reportError("Failed to create encryption", response.responseText, response.statusText) );
        return result;
    }
}

export = checkEncryptionKey; 

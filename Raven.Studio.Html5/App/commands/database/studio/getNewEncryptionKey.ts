import commandBase = require("commands/commandBase");

class getNewEncryptionKey extends commandBase {

    constructor() {
        super();
    }

    execute() {
        var key = this.query("/studio-tasks/new-encryption-key", null, null);
        return key;
    }
}

export = getNewEncryptionKey; 
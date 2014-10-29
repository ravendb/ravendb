import filesystem = require("models/filesystem/filesystem");

class uploadItem {
    id = ko.observable<string>("");
    fileName = ko.observable<string>("");
    public status = ko.observable<string>("");
    filesystem: filesystem;

    constructor(id: string, fileName: string, status: string, filesystem : filesystem) {
        this.id(id);
        this.fileName(fileName);
        this.status(status);
        this.filesystem = filesystem;
    }
}

export = uploadItem;
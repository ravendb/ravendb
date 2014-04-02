class uploadItem {
    id = ko.observable<string>("");
    fileName = ko.observable<string>("");
    public status = ko.observable<string>("");

    constructor(id: string, fileName: string, status: string) {
        this.id(id);
        this.fileName(fileName);
        this.status(status);
    }
}

export = uploadItem;
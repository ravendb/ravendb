class configurationDocument<T extends copyFromParentDto<any>> {

    localExists = ko.observable<boolean>();
    globalExists = ko.observable<boolean>();
    mergedDocument = ko.observable<T>();
    globalDocument = ko.observable<T>();
    etag = ko.observable<string>();
    metadata = ko.observable<any>();

    constructor(dto: configurationDocumentDto<T>) {
        this.localExists(dto.LocalExists);
        this.globalExists(dto.GlobalExists);
        this.mergedDocument(dto.MergedDocument);
        this.globalDocument(dto.GlobalDocument);
        this.etag(dto.Etag);
        this.metadata(dto.Metadata);
    }

    static fromDtoWithTransform<S, T extends copyFromParentDto<any>>(dto: configurationDocumentDto<S>, transform: { (x: S): T }): configurationDocument<T> {
        return new configurationDocument<T>({
            Etag: dto.Etag,
            MergedDocument: transform(dto.MergedDocument),
            Metadata: dto.Metadata,
            GlobalDocument: transform(dto.GlobalDocument),
            LocalExists: dto.LocalExists,
            GlobalExists: dto.GlobalExists
        });
    }

    isUsingGlobal() {
        var g = this.globalExists();
        var l = this.localExists();
        return g && !l;
    }

    copyFromGlobal() {
        if (this.globalExists()) {
            this.localExists(false);
            this.mergedDocument().copyFromParent(this.globalDocument());
        }
    }

}

export = configurationDocument;
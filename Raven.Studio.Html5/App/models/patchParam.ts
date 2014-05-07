class patchParam {

    key = ko.observable<string>();
    value = ko.observable<string>();

    constructor(dto: patchValueDto) {
        this.key(dto.Key);
        this.value(dto.Value);
    }

    static empty() {
        return new patchParam({Key: "", Value: ""});
    }
}

export = patchParam;
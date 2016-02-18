/// <reference path="../../../../typings/tsd.d.ts"/>

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

    toDto(): patchValueDto {
        return {
            Key: this.key(),
            Value: this.value()
        };
    }
}

export = patchParam;

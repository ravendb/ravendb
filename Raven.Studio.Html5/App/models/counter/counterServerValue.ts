class counterServerValue {
    serverUrl = ko.observable('');
    posCount  = ko.observable(0);
    negCount = ko.observable(0);

    constructor(dto: counterServerValueDto) {
        this.serverUrl(dto.ServerUrl);
        this.posCount(dto.Positive);
        this.negCount(dto.Negative);
    }
} 

export = counterServerValue;
/// <reference path="../../../../typings/tsd.d.ts"/>

class dataExplorationRequest {

    linq = ko.observable<string>();
    collection = ko.observable<string>();
    timeoutSeconds = ko.observable<number>();
    pageSize = ko.observable<number>();

    constructor(dto: dataExplorationRequestDto) {
        this.linq(dto.Linq);
        this.collection(dto.Collection);
        this.timeoutSeconds(dto.TimeoutSeconds);
        this.pageSize(dto.PageSize);
    }

    toDto(): dataExplorationRequestDto {
        return {
            Linq: this.linq(),
            Collection: this.collection(),
            TimeoutSeconds: this.timeoutSeconds(),
            PageSize: this.pageSize()
        };
    }

    static empty() {
        return new dataExplorationRequest({
            Linq: "from result in results \nselect result",
            Collection: null,
            PageSize: 1000,
            TimeoutSeconds: 60
        })
    }
}

export = dataExplorationRequest;

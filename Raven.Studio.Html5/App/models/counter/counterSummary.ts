class counterSummary {
    id = ko.observable<string>();
    Group: string;
    Name: string;
    Increments: number;
    Decrements: number;
    Total: number;

    constructor(dto: counterSummaryDto) {
        this.id(dto.Group + "/" + dto.CounterName);
        this.Group = dto.Group;
        this.Name = dto.CounterName;
        this.Increments = dto.Increments;
        this.Decrements = dto.Decrements;
        this.Total = dto.Total;
    }

    getDocumentPropertyNames(): Array<string> {
        return [ "Total", "Increments", "Decrements"];
    }

    getId() {
        return this.id();
    }

    getUrl() {
        //TODO: generate url
        return "123";
    }
} 

export = counterSummary;
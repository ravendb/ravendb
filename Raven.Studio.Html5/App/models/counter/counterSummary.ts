class counterSummary implements documentBase {
    static separator = "/";
    Id: string;
    Group: string;
    Name: string;
    Total: number; 

    constructor(dto: counterSummaryDto) {
        this.Id = dto.Group + counterSummary.separator + dto.CounterName;
        this.Group = dto.Group;
        this.Name = dto.CounterName;
        this.Total = dto.Total;
    }

    getEntityName() {
        return this.Group;
    }

    getDocumentPropertyNames(): Array<string> {
        return [ "Id", "Group", "Name", "Total"];
    }

    getId() {
        return this.Id;
    }

    getUrl() {
        return this.getId();
    }
} 

export = counterSummary;
class counterSummary implements documentBase {
    static separator = "/";
    GroupName: string;
    Name: string;
    Total: number; 

    constructor(dto: counterSummaryDto) {
        this.GroupName = dto.GroupName;
        this.Name = dto.CounterName;
        this.Total = dto.Total;
    }

    getEntityName() {
        return this.GroupName;
    }

    getDocumentPropertyNames(): Array<string> {
        return [ "Group", "Name", "Total"];
    }

    getId() {
        return this.Name;
    }

    getUrl() {
        return this.getId();
    }
} 

export = counterSummary;
/// <reference path="../../../typings/tsd.d.ts"/>

class counterSummary implements documentBase {
    Total: number; 

    constructor(dto: counterSummaryDto, private isAllGroupsGroup: boolean = false) {
        this["Counter Name"] = dto.CounterName;
        this["Group Name"] = dto.GroupName;
        this.Total = dto.Total;
    }

    getEntityName() {
        return this["Group Name"];
    }

    getDocumentPropertyNames(): Array<string> {
        var properties = ["Counter Name"];
        if (this.isAllGroupsGroup)
            properties.push("Group Name");
        properties.push("Total");
        return properties;
    }

    getId() {
        return this["Counter Name"];
    }

    getUrl() {
        return this.getId();
    }
} 

export = counterSummary;

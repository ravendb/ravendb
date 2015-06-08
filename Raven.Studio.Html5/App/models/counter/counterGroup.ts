import counterStorage = require("models/counter/counterStorage");
import pagedList = require("common/pagedList");
import getCountersCommand = require("commands/counter/getCountersCommand");
import pagedResultSet = require("common/pagedResultSet");

class counterGroup {
    static allGroupsGroupName = "All Groups";
    private countersList: pagedList;
    countersCount = ko.observable<number>(0);
    countersCountWithThousandsSeparator = ko.computed(() => this.countersCount().toLocaleString());
    isAllGroupsGroup = false;

    constructor(public name: string, private ownerCounterStorage: counterStorage, count: number = 0) {
        this.countersCount(count);
        this.isAllGroupsGroup = name === counterGroup.allGroupsGroupName;
    }

    activate() {
		ko.postbox.publish("ActivateGroup", this);
    }

    getCounters() {
        if (!this.countersList) {
            this.countersList = this.createPagedList();
        }

        return this.countersList;
    }

    static createAllGroupsCollection(ownerCounterStorage: counterStorage): counterGroup {
        return new counterGroup(counterGroup.allGroupsGroupName, ownerCounterStorage);
    }

    static fromDto(dto: counterGroupDto, storage: counterStorage): counterGroup {
        return new counterGroup(dto.Name, storage, dto.Count);
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchCounters(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }

    private fetchCounters(skip: number, take: number): JQueryPromise<pagedResultSet> {
        var group = this.isAllGroupsGroup ? null : this.name;
        return new getCountersCommand(this.ownerCounterStorage, skip, take, group).execute();
    }
} 

export = counterGroup;
import counterStorage = require("models/counter/counterStorage");
import pagedList = require("common/pagedList");

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

    static createAllGroupsCollection(ownerCounterStorage: counterStorage): counterGroup {
        return new counterGroup(counterGroup.allGroupsGroupName, ownerCounterStorage);
    }

    static fromDto(dto: counterGroupDto, storage: counterStorage): counterGroup {
        return new counterGroup(dto.Name, storage, dto.Count);
    }
} 

export = counterGroup;
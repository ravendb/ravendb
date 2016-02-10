import counterStorage = require("models/counter/counterStorage");
import pagedList = require("common/pagedList");
import getCountersCommand = require("commands/counter/getCountersCommand");
import pagedResultSet = require("common/pagedResultSet");
import cssGenerator = require("common/cssGenerator");

class counterGroup implements ICollectionBase {
    colorClass = "";
    static allGroupsGroupName = "All Groups";
    private countersList: pagedList;
    private static groupColorMaps: resourceStyleMap[] = [];
    countersCount = ko.observable<number>(0);
    countersCountWithThousandsSeparator = ko.computed(() => this.countersCount().toLocaleString());
    isAllGroupsGroup = false;
    
    constructor(public name: string, private ownerCounterStorage: counterStorage, count: number = 0) {
        this.countersCount(count);
        this.isAllGroupsGroup = name === counterGroup.allGroupsGroupName;
        this.colorClass = counterGroup.getGroupCssClass(name, ownerCounterStorage);
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

    invalidateCache() {
        var countersList = this.getCounters();
        countersList.invalidateCache();
    }

    static createAllGroupsCollection(ownerCounterStorage: counterStorage): counterGroup {
        return new counterGroup(counterGroup.allGroupsGroupName, ownerCounterStorage);
    }

    static fromDto(dto: counterGroupDto, storage: counterStorage): counterGroup {
        return new counterGroup(dto.Name, storage, dto.Count);
    }

    static getGroupCssClass(entityName: string, cs: counterStorage): string {
        if (entityName === counterGroup.allGroupsGroupName) {
            return "all-documents-collection";
        }

        return cssGenerator.getCssClass(entityName, counterGroup.groupColorMaps, cs);
    }

    private createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchCounters(skip, take);
        var list = new pagedList(fetcher);
        list.collectionName = this.name;
        return list;
    }

    private fetchCounters(skip: number, take: number): JQueryPromise<pagedResultSet<any>> {
        var group = this.isAllGroupsGroup ? null : this.name;
        return new getCountersCommand(this.ownerCounterStorage, skip, take, group).execute();
    }
} 

export = counterGroup;

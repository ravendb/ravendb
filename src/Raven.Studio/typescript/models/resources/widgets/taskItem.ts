
class taskItem {
    taskType = ko.observable<StudioTaskType>();
    taskCount = ko.observable<number>();
    
    databaseName = ko.observable<string>();
    nodeTags = ko.observableArray<string>();

    isTitleItem = ko.observable<boolean>(false);
    even = false;

    constructor(type: StudioTaskType, count: number, dbName?: string, nodes?: string[]) {
        this.taskType(type);
        this.databaseName(dbName);
        this.taskCount(count);
        this.nodeTags(nodes);
        
        if (!dbName) {
            this.isTitleItem(true);
        }
    }

    updateWith(countToAdd: number, nodeToAdd: string) {
        this.taskCount(this.taskCount() + countToAdd);

        const foundTag = this.nodeTags().find(x => x === nodeToAdd);
        if (!foundTag) {
            this.nodeTags.push(nodeToAdd);
        }
    }
    
    static itemFromRaw(rawItem: rawTaskItem): taskItem {
        return new taskItem(rawItem.type, rawItem.count, rawItem.dbName, [rawItem.node]);
    }

    static createNodeTagsProvider() {
        return (item: taskItem) => item.nodeTags();
    }
}

export = taskItem;

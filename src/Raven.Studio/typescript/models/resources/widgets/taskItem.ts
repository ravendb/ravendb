
class taskItem {
    taskType = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType>();
    taskCount = ko.observable<number>();
    
    databaseName = ko.observable<string>();
    nodeTags = ko.observableArray<string>();

    isTitleItem = ko.observable<boolean>(false);
    even: boolean = false;

    constructor(type: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, count: number, dbName?: string, nodes?: string[]) {
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
}

export = taskItem;

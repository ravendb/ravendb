class indexes {

    indexGroups = ko.observableArray();

    constructor() {

        // TODO: fill this with real data.
        this.indexGroups.pushAll([
            { name: 'FakeIndexGroup1' },
            { name: 'FakeIndexGroup2' }
        ]);
    }
    
    activate() {
    }

    navigateToQuery() {
        console.log("TODO: implement");
    }

    navigateToNewIndex() {
        console.log("TODO: implement");
    }

    collapseAll() {
        console.log("TODO: implement");
    }

    expandAll() {
        console.log("TODO: implement");
    }
}

export = indexes;
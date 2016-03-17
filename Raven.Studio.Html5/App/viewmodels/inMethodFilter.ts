import searchDialogViewModel = require("viewmodels/filesystem/searchDialogViewModel");

class inMethodFilterElement {
    value = ko.observable('');
    focus = ko.observable<boolean>(false);
}

class inMethodFilter extends searchDialogViewModel {
    
    public applyFilterTask = $.Deferred();
    elements = ko.observableArray<inMethodFilterElement>();
    deleteEnabled = ko.computed(() => this.elements().length > 1);

    constructor(private label: string) {        
        super([ko.observable("")]);
        this.newElement();
    }

    applyFilter() {
        this.applyFilterTask.resolve(this.projectElementsToStringArray());
        
        this.close();
    }

    enabled(): boolean {
        return true;
    }

    newElement() {
        var element = new inMethodFilterElement();
        this.elements.push(element);
        element.focus(true);
    }

    private removeElement(element: inMethodFilterElement) {
        this.elements.remove(element);
    }

    projectElementsToStringArray(): string[] {
        return this.elements().map(x => x.value());
    }
}

export = inMethodFilter;   

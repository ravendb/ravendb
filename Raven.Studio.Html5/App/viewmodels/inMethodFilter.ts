import searchDialogViewModel = require("viewmodels/filesystem/files/searchDialogViewModel");

class inMethodFilter extends searchDialogViewModel {
    
    public applyFilterTask = $.Deferred();
    label = "";
    elements = ko.observableArray<KnockoutObservable<string>>();

    constructor(label: string) {        
        super([ko.observable("")]);
        this.label = label;
        this.addElement("");
    }

    applyFilter() {
        this.applyFilterTask.resolve(this.projectElementsToStringArray());
        
        this.close();
    }

    enabled(): boolean {
        return true;
    }

    removeElement(index: number) {
        this.elements.splice(index, 1);
    }

    addElement(element: string) {
        var newElement = ko.observable<string>(element);
        this.elements.push(newElement);
    }

    isLastElement(index: number): boolean {
        return index === this.elements.length - 1;
    }

    projectElementsToStringArray(): string[] {
        return this.elements().map((element: KnockoutObservable<string>) => element());
    }
}

export = inMethodFilter;   
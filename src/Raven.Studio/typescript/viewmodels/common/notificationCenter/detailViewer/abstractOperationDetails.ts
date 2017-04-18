import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import operation = require("common/notifications/models/operation");
import notificationCenter = require("common/notifications/notificationCenter");

abstract class abstractOperationDeatils extends dialogViewModelBase {

    protected readonly op: operation;
    protected readonly killFunction: () => void;

    killable: KnockoutComputed<boolean>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super();
        this.bindToCurrentInstance("close", "killOperation");
        this.op = op;
        this.killFunction = () => notificationCenter.killOperation(op);
    }

    //TODO: elapsed time

    protected initObservables() {
        this.killable = ko.pureComputed(() => !this.op.isCompleted());
    }

    killOperation() {
        this.killFunction();
        //TODO: spinner
    }

}

export = abstractOperationDeatils;

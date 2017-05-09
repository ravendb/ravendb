import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import operation = require("common/notifications/models/operation");
import notificationCenter = require("common/notifications/notificationCenter");
import viewHelpers = require("common/helpers/view/viewHelpers");

abstract class abstractOperationDetails extends dialogViewModelBase {

    protected readonly op: operation;
    protected readonly killFunction: () => void;
    protected readonly openDetails: () => void;

    operationFailed: KnockoutComputed<boolean>;
    killable: KnockoutComputed<boolean>;
    errorMessages: KnockoutComputed<string[]>;

    spinners = {
        kill: ko.observable<boolean>(false)
    }

    constructor(op: operation, notificationCenter: notificationCenter) {
        super();
        this.bindToCurrentInstance("close", "killOperation");
        this.op = op;
        this.killFunction = () => notificationCenter.killOperation(op);
        this.openDetails = () => notificationCenter.openDetails(op);

        this.registerDisposable(this.op.status.subscribe(status => {
            if (status === "Canceled") {
                this.close();
            }
        }));
    }

    protected initObservables() {
        this.killable = ko.pureComputed(() => !this.op.isCompleted());
        this.operationFailed = ko.pureComputed(() => this.op.status() === "Faulted");
        this.errorMessages = ko.pureComputed(() => {
            if (this.operationFailed()) {
                const exceptionResult = this.op.result() as Raven.Client.Documents.Operations.OperationExceptionResult;
                return [exceptionResult.Message, exceptionResult.Error];
            }
            return [];
        });
    }

    killOperation() {
        this.close();
        viewHelpers.confirmationMessage("Are you sure?", "Do you want to abort current operation?", ["No", "Yes"], true)
            .done((result: confirmDialogResult) => {
                if (result.can) {
                    this.spinners.kill(true);
                    this.killFunction();
                    // note we don't set kill back to false - we close details view when operation is cancelled
                } else {
                    this.openDetails();
                }
            });
    }

}

export = abstractOperationDetails;

import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import operation = require("common/notifications/models/operation");
import notificationCenter = require("common/notifications/notificationCenter");
import generalUtils = require("common/generalUtils");
import moment = require("moment");

abstract class abstractOperationDetails extends dialogViewModelBase {
    
    footerPartialView = require("views/common/notificationCenter/detailViewer/operations/footerPartial.html");

    protected readonly op: operation;
    protected readonly killFunction: () => JQueryPromise<confirmDialogResult>;
    protected readonly openDetails: () => void;

    operationFailed: KnockoutComputed<boolean>;
    killable: KnockoutComputed<boolean>;
    errorMessages: KnockoutComputed<string[]>;

    spinners = {
        kill: ko.observable<boolean>(false)
    };

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
        
        // kill function handles confirmation 
        this.killFunction()
            .done(result => {
                if (result.can) {
                    this.spinners.kill(true);
                } else {
                    this.openDetails();
                }
            });
    }
    
    protected calculateProcessingSpeed(processed: number): number {
        const durationInSeconds = this.op.durationInSeconds();
        return abstractOperationDetails.calculateProcessingSpeed(durationInSeconds, processed);
    }

    static calculateProcessingSpeed(durationInSeconds: number, processed: number): number {
        if (durationInSeconds <= 0) {
            return 0;
        }

        const result = processed / durationInSeconds;
        if (result <= 0) {
            return 0;
        }

        if (result < 1) {
            return result;
        }

        return Math.floor(result);
    }

    protected getEstimatedTimeLeftFormatted(processed: number, total: number): string {
        const processingSpeed = this.calculateProcessingSpeed(processed);
        if (processingSpeed === 0) {
            return "N/A";
        }

        const leftToProcess = total - processed;
        const leftInSeconds = leftToProcess / processingSpeed;
        if (leftInSeconds === 0) {
            return "N/A";
        }

        const formattedDuration = generalUtils.formatDuration(moment.duration(leftInSeconds * 1000), true, 2, true);
        if (!formattedDuration) {
            return "N/A";
        }

        return `${formattedDuration}`;
    }
}

export = abstractOperationDetails;

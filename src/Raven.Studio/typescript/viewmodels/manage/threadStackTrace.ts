import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import getStackTraceForThreadCommand = require("commands/maintenance/getStackTraceForThreadCommand");
import generalUtils = require("common/generalUtils");

class threadStackTrace extends dialogViewModelBase {

    threadId = ko.observable<number>();
    threadName = ko.observable<string>();
    threadType = ko.observable<string>();
    
    cpuUsage = ko.observable<string>();
    stackTrace = ko.observableArray<string>([]);

    isThreadAlive = ko.observable<boolean>();
    
    dialogContainer: Element;

    spinners = {
        loading: ko.observable<boolean>(false)
    };

    constructor(threadId: number, threadName: string) {
        super();
        
        this.threadId(threadId);
        this.threadName(threadName);
    }
    
    compositionComplete() {
        super.compositionComplete();
        this.dialogContainer = document.getElementById("threadStackTraceDialog");
    }
    
    activate() {
        return this.loadStackTrace(this.threadId());
    }

    private loadStackTrace(threadId: number): JQueryPromise<threadStackTraceResponseDto> {
        this.spinners.loading(true);
        
        return new getStackTraceForThreadCommand(threadId)
            .execute()
            .done((results: threadStackTraceResponseDto) => {
                if (results.Results.length) {
                    this.isThreadAlive(true);
                    this.stackTrace(results.Results[0].StackTrace);
                    this.threadType(results.Results[0].ThreadType);
                    this.cpuUsage(this.getCpuUsage(results.Threads));
                } else {
                    this.isThreadAlive(false);
                    this.stackTrace([]);
                    this.threadName("N/A");
                    this.threadType("N/A");
                    this.cpuUsage("N/A");
                }
            })
            .always(() => this.spinners.loading(false));
    }

    getCpuUsage(threadsInfo: Array<Raven.Server.Dashboard.ThreadInfo>): string {
        const matchedThread = threadsInfo.find(x => x.Id === this.threadId())
        return matchedThread ? `${(matchedThread.CpuUsage === 0 ? "0" : generalUtils.formatNumberToStringFixed(matchedThread.CpuUsage, 2))}%` : "N/A";
    }
    
    copyStackTrace(): void {
        copyToClipboard.copy(this.stackTrace().toString(), "Stack trace has been copied to clipboard", this.dialogContainer);
    }

    isUserCode(line: string): boolean {
        return generalUtils.isRavenDBCode(line);
    }
    
    refreshStackTrace(): JQueryPromise<threadStackTraceResponseDto> {
        return this.loadStackTrace(this.threadId());
    }
}

export = threadStackTrace;

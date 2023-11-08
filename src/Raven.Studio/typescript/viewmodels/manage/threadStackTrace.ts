import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import getStackTraceForThreadCommand = require("commands/maintenance/getStackTraceForThreadCommand");
import generalUtils = require("common/generalUtils");

class threadStackTrace extends dialogViewModelBase {

    view = require("views/manage/threadStackTrace.html");
    
    threadId = ko.observable<number>();
    threadName = ko.observable<string>();
    threadType = ko.observable<string>();
    
    cpuUsage = ko.observable<string>();
    stackTrace = ko.observableArray<string>([]);

    isThreadAlive = ko.observable<boolean>();
    
    dialogContainer: HTMLElement;

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
        
        this.loadStackTrace(this.threadId());
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
                    const [name, cpu] = this.getInfoFromThreadsList(results.Threads);
                    this.threadName(name);
                    this.cpuUsage(cpu);
                } else {
                    this.isThreadAlive(false);
                }
            })
            .always(() => this.spinners.loading(false));
    }

    getInfoFromThreadsList(threadsList: Array<Raven.Server.Dashboard.ThreadInfo>): [string, string] {
        const matchedThread = threadsList.find(x => x.Id === this.threadId())
        
        const cpu =  matchedThread ? `${(matchedThread.CpuUsage === 0 ? "0" : generalUtils.formatNumberToStringFixed(matchedThread.CpuUsage, 2))}%` : "N/A";
        const name = matchedThread ? matchedThread.Name : "N/A";
        
        return [name, cpu];
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

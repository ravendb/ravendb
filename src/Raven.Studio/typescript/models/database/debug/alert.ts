/// <reference path="../../../../typings/tsd.d.ts"/>

class alert {

    id: string;
    key: string;
    message: string;
    read: boolean;
    severity: Raven.Server.Documents.AlertSeverity;
    type: Raven.Server.Documents.AlertType;
    createdAt: string;
    dismissedUntil: string;
    content: Raven.Server.Documents.IAlertContent;

    global: boolean;

    isError: boolean;
    isWarning: boolean;
    isInfo: boolean;

    constructor(dto: Raven.Server.Documents.Alert) {
        this.id = dto.Id;
        this.key = dto.Key;
        this.message = dto.Message;
        this.read = dto.Read;
        this.severity = dto.Severity;
        this.type = dto.Type;
        this.createdAt = dto.CreatedAt;
        this.dismissedUntil = dto.DismissedUntil;
        this.content = dto.Content;

        this.initStatus();
    }

    private initStatus() {
        this.isError = this.severity === ("Error" as Raven.Server.Documents.AlertSeverity);
        this.isWarning = this.severity === ("Warning" as Raven.Server.Documents.AlertSeverity);
        this.isInfo = this.severity === ("Info" as Raven.Server.Documents.AlertSeverity);
    }


}

export = alert;

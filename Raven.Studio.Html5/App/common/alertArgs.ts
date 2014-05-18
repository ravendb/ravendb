import alertType = require("common/alertType");

class alertArgs {
    public id: string;
    private detailsObject: any;
    private parsedErrorInfo: any;

    constructor(public type: alertType, public title: string, public details: string = "", public httpStatusText: string = "") {
        var hashString = (title + details)
            .hashCode()
            .toString();

        this.id = "alert_" + hashString;
    }

    get errorMessage(): string {
        var error = this.errorInfo;
        if (error && error.message) {
            return error.message;
        }

        return null;
    }

    get errorInfo(): { message: string; stackTrace: string; url: string; } {
        if (this.parsedErrorInfo) {
            return this.parsedErrorInfo;
        }

        if (this.type !== alertType.danger && this.type !== alertType.warning) {
            return null;
        }

        // See if we can tease out an error message from the details string.
        var detailsObj = this.getDetailsObject();
        if (detailsObj) {
            var error: string = detailsObj.Error;
            if (error && typeof error === "string") {
                var indexOfStackTrace = error.indexOf("\r\n");

                if (indexOfStackTrace !== -1) {
                    this.parsedErrorInfo = {
                        message: detailsObj.Message?detailsObj.Message:error.substr(0, indexOfStackTrace),
                        stackTrace: detailsObj.Message?error:error.substr(indexOfStackTrace + "\r\n".length),
                        url: detailsObj.Url || ""
                    };
                } else {
                    this.parsedErrorInfo = {
                        message: detailsObj.Message?detailsObj.Message:error,
                        stackTracke: error,
                        url: detailsObj.Url
                    }
                }
            }
        }

        return this.parsedErrorInfo;
    }

    getDetailsObject(): any {
        if (this.detailsObject) {
            return this.detailsObject;
        }

        if (this.details) {
            try {
                this.detailsObject = JSON.parse(this.details);
            } catch (error) {
                return null;
            }
        }

        return this.detailsObject;
    }
}

export = alertArgs;
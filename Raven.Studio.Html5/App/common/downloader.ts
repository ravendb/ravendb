import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher"); 
import resource = require("models/resources/resource");
import appUrl = require("common/appUrl");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");

class downloader {
    $downloadFrame = $("#downloadFrame");
    $downloadForm = $("#downloadForm");
    $downloadFormOptions = $("#DownloadOptions");
    $downloadFormTaskId = $("#ProgressTaskId");

    download(rs: resource, url: string) {
        new getSingleAuthTokenCommand(rs).execute().done((token: singleAuthToken) => {
            var authToken = (url.indexOf("?") === -1 ? "?" : "&") + "singleUseAuthToken=" + token.Token;
            this.$downloadFrame.attr("src", url + authToken);
        }).fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not get single auth token for download.", errorThrown));
    }

    downloadByPost(rs: resource, url: string, requestData,
            isDownloading: KnockoutObservable<boolean>, downloadStatus: KnockoutObservable<string>) {
        new getSingleAuthTokenCommand(rs).execute().done((token: singleAuthToken) => {

            new getNextOperationId(rs).execute()
                .done((operationId: number) => {
                    var authToken = (url.indexOf("?") === -1 ? "?" : "&") + "singleUseAuthToken=" + token.Token;
                    this.$downloadForm.attr("action", appUrl.forResourceQuery(rs) + url + authToken);
                    this.$downloadFormOptions.val(JSON.stringify(requestData));
                    this.$downloadFormTaskId.val(operationId.toString());
                    this.$downloadForm.submit();

                    this.$downloadFormTaskId.val("-1");

                    setTimeout(() => this.waitForOperationToComplete(rs, operationId, isDownloading, downloadStatus), 500);
                })
                .fail((qXHR, textStatus, errorThrown) => {
                    messagePublisher.reportError("Could not get next task id.", errorThrown);
                    isDownloading(false);
                });

        }).fail((qXHR, textStatus, errorThrown) => {
            messagePublisher.reportError("Could not get single auth token for download.", errorThrown);
            isDownloading(false);
        });
    }

    private waitForOperationToComplete(rs: resource, operationId: number,
        isDownloading: KnockoutObservable<boolean>,
        downloadStatus: KnockoutObservable<string>,
        tries = 0) {

        new getOperationStatusCommand(rs, operationId)
            .execute()
            .done((result: dataDumperOperationStatusDto) =>
                this.downloadStatusRetrieved(rs, operationId, result, isDownloading, downloadStatus))
            .fail((qXHR, textStatus, errorThrown) => {
                tries++;
                if (tries > 0 && tries < 5) {
                    messagePublisher.reportError("Could not get task id: " + operationId + " for download.", errorThrown);
                    setTimeout(() => this.waitForOperationToComplete(rs, operationId, isDownloading, downloadStatus, tries), 1000);
                    return;
                }

                isDownloading(false);
                return;
            });
    }

    private downloadStatusRetrieved(rs: resource, operationId: number, result: dataDumperOperationStatusDto,
        isDownloading: KnockoutObservable<boolean>, downloadStatus: KnockoutObservable<string>) {
        if (result.Completed) {
            if (result.ExceptionDetails == null && result.State != null && result.State.Progress != null) {
                downloadStatus("Export finished, " + result.State.Progress.toLocaleLowerCase());
                messagePublisher.reportSuccess("Successfully downloaded data for " + rs.name);
            } else if (result.Canceled) {
                downloadStatus("Download was canceled!");
            }
            else {
                downloadStatus("Failed to download data, see recent errors for details!");
                messagePublisher.reportError("Failed to download data!", result.ExceptionDetails);
            }

            isDownloading(false);
        }
        else {
            if (!!result.State && result.State.Progress) {
                downloadStatus("Exporting, " + result.State.Progress.toLocaleLowerCase());
            }
            setTimeout(() => this.waitForOperationToComplete(rs, operationId, isDownloading, downloadStatus), 1000);
        }
    }

    reset() {
        this.$downloadFrame.attr("src", "");

        this.$downloadForm.removeAttr("action");
        this.$downloadFormOptions.removeAttr("value");
        this.$downloadFormTaskId.val("-1");
    }
}

export = downloader

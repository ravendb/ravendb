import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher"); 
import resource = require("models/resources/resource");
import appUrl = require("common/appUrl");

class downloader {
    $downloadFrame = $("#downloadFrame");

    download(resource: resource, url: string) {
        new getSingleAuthTokenCommand(resource).execute().done((token: singleAuthToken) => {
            var authToken = (url.indexOf("?") === -1 ? "?" : "&") + "singleUseAuthToken=" + token.Token;
            this.$downloadFrame.attr("src", url + authToken);
        }).fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not get single auth token for download.", errorThrown));
    }

    downloadByPost(resource: resource, url: string, requestData, isDownloading: KnockoutObservable<boolean>) {
        new getSingleAuthTokenCommand(resource).execute().done((token: singleAuthToken) => {
            var authToken = (url.indexOf("?") === -1 ? "?" : "&") + "singleUseAuthToken=" + token.Token;

            var xhttp = new XMLHttpRequest();
            var htmlElement: HTMLElement = this.$downloadFrame[0];

            xhttp.onreadystatechange = () => {

                if (xhttp.readyState !== 4) {
                    if (xhttp.status === 500 && xhttp.readyState < 3) {
                        xhttp.responseType = "text";
                    }
                    return;
                }

                if (xhttp.status === 500) {
                    messagePublisher.reportError(xhttp.statusText, xhttp.responseText);
                }
                else if (xhttp.status === 200) {
                    var responseHeader = xhttp.getResponseHeader("Content-Disposition");
                    var splitted = responseHeader.split("filename=\"");
                    var fileName = splitted[1].slice(0, splitted[1].length - 1);

                    if (navigator.appVersion.toString().indexOf(".NET") > 0) {
                        //for IE
                        var type = xhttp.getResponseHeader("Content-Type");
                        var blob = new Blob([xhttp.response], { type: type });
                        window.navigator.msSaveBlob(blob, fileName);
                    }
                    else {
                        //trick for making a downloadable link
                        var a = document.createElement("a");
                        a.href = URL.createObjectURL(xhttp.response);
                        (<any>a).download = fileName;
                        a.style.display = "none";
                        htmlElement.appendChild(a);
                        a.click();
                        }
                }
            };

            //connection errors handling
            xhttp.onerror = (e: Event) => {
                messagePublisher.reportError("An error occured while sending the download request!", "Url: " + url);
                isDownloading(false);
            };
            xhttp.onabort = (e: Event) => {
                messagePublisher.reportError("The download request was aborted!", "Url: " + url);
                isDownloading(false);
            };
            xhttp.addEventListener("loadend", () => isDownloading(false));

            //post data to URL which handles post requests
            xhttp.open("POST", appUrl.forResourceQuery(resource) + url + authToken);
            xhttp.setRequestHeader("Content-Type", "application/json");
            xhttp.responseType = "blob";
            xhttp.send(JSON.stringify(requestData));
        }).fail((qXHR, textStatus, errorThrown) => {
            isDownloading(false);
            messagePublisher.reportError("Could not get single auth token for download.", errorThrown);
        });
    }

    reset() {
        this.$downloadFrame.attr("src", "");
        this.$downloadFrame.empty();
    }
}

export = downloader

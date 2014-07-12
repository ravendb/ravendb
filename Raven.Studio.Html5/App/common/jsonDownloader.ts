class jsonDownloader {

    private static cleanup(domCacheElementName: string) {
        // clean previous elements (in any)
        $("#" + domCacheElementName).remove();
    }


    private static createLinkAndStartDownload(blob: Blob, filename: string, domCacheElementName: string) {
        if (navigator && navigator.msSaveBlob) {
            navigator.msSaveBlob(blob, filename);
        } else {
            var blobUrl = URL.createObjectURL(blob);
            var a = document.createElement('a');
            a.id = "#" + domCacheElementName;
            (<any>a).download = filename;
            a.href = blobUrl;
            document.body.appendChild(a); // required by firefox
            a.click();
        }
    }

    static downloadAsJson(object: any, filename: string, domCacheElementName: string = "jsonLink") {
        jsonDownloader.cleanup(domCacheElementName);
        var modelAsString = JSON.stringify(object, null, 2);
        var blob = new Blob([modelAsString], { type: 'application/json' });
        jsonDownloader.createLinkAndStartDownload(blob, filename, domCacheElementName);

    }
} 

export = jsonDownloader;

/// <reference path="../../typings/tsd.d.ts" />

class fileDownloader {

    private static cleanup(domCacheElementName: string) {
        // clean previous elements (in any)
        $("#" + domCacheElementName).remove();
    }

    private static createLinkAndStartDownload(blob: Blob, filename: string, domCacheElementName: string) {
        if (navigator && navigator.msSaveBlob) {
            navigator.msSaveBlob(blob, filename);
        } else {
            const blobUrl = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.id = "#" + domCacheElementName;
            (<any>a).download = filename;
            a.href = blobUrl;
            document.body.appendChild(a); // required by firefox
            a.click();
        }
    }
    
    static downloadAsTxt(buffer: string | Uint8Array, filename: string, domCacheElementName: string = "link") {
        domCacheElementName = _.snakeCase(domCacheElementName);
        fileDownloader.cleanup(domCacheElementName);
        const blob = new Blob([buffer], { type: 'text/plain' });
        fileDownloader.createLinkAndStartDownload(blob, filename, domCacheElementName);
    }

    static downloadAsJson(object: any, filename: string, domCacheElementName: string = "link", replacer: (key: string, value: any) => any = null) {
        domCacheElementName = _.snakeCase(domCacheElementName);
        fileDownloader.cleanup(domCacheElementName);
        const modelAsString = JSON.stringify(object, replacer, 2);
        const blob = new Blob([modelAsString], { type: 'application/json' });
        fileDownloader.createLinkAndStartDownload(blob, filename, domCacheElementName);
    }

    static downloadAsZip(buffer:any, filename: string, domCacheElementName: string = "link") {
        fileDownloader.cleanup(domCacheElementName);
        const blob = new Blob([buffer], { type: 'application/zip' });
        fileDownloader.createLinkAndStartDownload(blob, filename, domCacheElementName);
    }
} 

export = fileDownloader;

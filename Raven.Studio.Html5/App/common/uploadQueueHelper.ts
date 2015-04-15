import filesystem = require("models/filesystem/filesystem");
import uploadItem = require("models/filesystem/uploadItem");

class uploadQueueHelper {

    static uploadingStatus = "Uploading...";
    static failedStatus = "Failed";
    static uploadedStatus = "Uploaded";
    static queuedStatus = "Queued";
    static localStorageUploadQueueKey = "ravenFs-uploadQueue.";

    static stringifyUploadQueue(queue: uploadItem[]): string {
        return ko.toJSON(queue);
    }

    static parseUploadQueue(queue: string, fs: filesystem): uploadItem[] {
        var stringArray: any[] = JSON.parse(queue);
        var uploadQueue:uploadItem[]  = [];

        for (var i = 0; i < stringArray.length; i++) {
            uploadQueue.push(new uploadItem(stringArray[i]["id"], stringArray[i]["fileName"],
                stringArray[i]["status"], fs));
        }

        return uploadQueueHelper.sortUploadQueue(uploadQueue);
    }

    static sortUploadQueue(uploadQueue: uploadItem[]): uploadItem[]{
        return uploadQueue.sort((a, b) => {
            if (a.status() === uploadQueueHelper.failedStatus) {
                return -1;
            }
            else if (a.status() === uploadQueueHelper.uploadingStatus)
                if (b.status() === uploadQueueHelper.failedStatus)
                    return 1;
                else
                    return -1;
            else if (a.status() === uploadQueueHelper.queuedStatus) {
                if (b.status() === uploadQueueHelper.failedStatus || b.status() === uploadQueueHelper.uploadingStatus)
                    return 1;
                else
                    return -1;
            }
            else
                return 1;
        });
    }

    static updateLocalStorage(x: uploadItem[], fs: filesystem) {
        window.localStorage.setItem(uploadQueueHelper.localStorageUploadQueueKey + fs.name, uploadQueueHelper.stringifyUploadQueue(x));
    }

    static updateQueueStatus(guid: string, status: string, queue: uploadItem[]) {
        var items = ko.utils.arrayFilter(queue, (i: uploadItem) => {
            return i.id() === guid
        });
        if (items) {
            items[0].status(status);
        }

        queue = uploadQueueHelper.sortUploadQueue(queue);;
    }
}
export = uploadQueueHelper;
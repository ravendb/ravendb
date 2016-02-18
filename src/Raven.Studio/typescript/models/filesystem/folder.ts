/// <reference path="../../../typings/tsd.d.ts"/>

class folder {

    name: string;

    constructor(public path: string) {
        if (path) {
            var lastSlash = path.lastIndexOf("/");
            if (lastSlash > 0) {
                this.name = path.substring(lastSlash + 1);
            }
            else {
                this.name = path.substring(1);
            }
        }
    }

    static getFolderFromFilePath(filePath: string): folder {
        var lastSlash = filePath.lastIndexOf("/");
        if (lastSlash == 0) {
            return new folder("/");
        }
        if (lastSlash > 0) {
            return new folder(filePath.substring(0, lastSlash));
        }
        return null;
    }

    getSubpathsFrom(basePath: string) : folder[] {
        if (basePath) {

            if (basePath[basePath.length - 1] != "/") {
                basePath = basePath + "/"
            }

            var restOfPath = this.path.replace(basePath, "");

            var subPaths = [new folder(basePath)];
            var lastIndexOfSlash = restOfPath.indexOf("/");
            for (var i = 0; i < restOfPath.count("/"); i++) {
                if (lastIndexOfSlash > 0) {
                    subPaths.push(new folder(basePath+restOfPath.substring(0, lastIndexOfSlash)));
                }

                lastIndexOfSlash = restOfPath.indexOf("/", lastIndexOfSlash + 1);
            }
            subPaths.push(new folder(this.path));

            return subPaths;
        }

        return null;
    }

    isFileAtFolderLevel(filePath: string): boolean {
        var fileToFolder = folder.getFolderFromFilePath(filePath);
        return fileToFolder && this.path === fileToFolder.path
    }

}

export = folder;

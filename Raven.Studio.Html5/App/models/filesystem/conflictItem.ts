import conflictHistory = require("models/filesystem/conflictHistory");

class conflictItem {

    remoteHistory: conflictHistory[];
    currentHistory: conflictHistory[];

    constructor(public fileName: string, public remoteServerUrl: string)
    {
        this.remoteHistory = [];
        this.currentHistory = [];
    }

    static fromConflictItemDto(dto: filesystemConflictItemDto) : conflictItem {
        var item = new conflictItem(dto.FileName, dto.RemoteServerUrl);
        if (dto.RemoteHistory != null) {
            item.remoteHistory.pushAll(dto.RemoteHistory.map(x => new conflictHistory(x.Version, x.ServerId)));
        }
        if (dto.CurrentHistory != null) {
            item.currentHistory.pushAll(dto.CurrentHistory.map(x => new conflictHistory(x.Version, x.ServerId)));
        }
        return item;
    }
}

export = conflictItem;
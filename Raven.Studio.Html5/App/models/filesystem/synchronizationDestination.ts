
import filesystem = require("models/filesystem/filesystem");

class synchronizationDestination implements synchronizationDestinationDto {

    ServerUrl: string;
    FileSystem: string;
    Username: string;
    Password: string;
    Domain: string;
    ApiKey: string;
    
    constructor(fs: filesystem, destination: string) {
        this.FileSystem = fs.name;
        this.ServerUrl = destination;
    }
}

export = synchronizationDestination;

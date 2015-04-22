import commandBase = require("commands/commandBase");
import windowsAuthSetup = require("models/auth/windowsAuthSetup");

class getWindowsAuthCommand extends commandBase {
    
    execute(): JQueryPromise<windowsAuthSetup> {
        return this.query(
            "/docs",
            { id: "Raven/Authorization/WindowsSettings" },
            null,
            (dto: windowsAuthDto) => new windowsAuthSetup(dto)
        );
    }
}

export = getWindowsAuthCommand;
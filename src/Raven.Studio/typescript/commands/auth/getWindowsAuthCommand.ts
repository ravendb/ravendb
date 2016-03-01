import commandBase = require("commands/commandBase");
import windowsAuthSetup = require("models/auth/windowsAuthSetup");

class getWindowsAuthCommand extends commandBase {
    
    execute(): JQueryPromise<windowsAuthSetup> {
        return this.query(
            "/document",
            { id: "Raven/Authorization/WindowsSettings" },
            null,
            (dto: windowsAuthDto) => new windowsAuthSetup(dto)
        );
    }
}

export = getWindowsAuthCommand;

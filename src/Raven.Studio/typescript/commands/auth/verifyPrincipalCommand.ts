import commandBase = require("commands/commandBase");

type verifyResultDto = {
    Valid: boolean;
}

class verifyPrincipalCommand extends commandBase {
    
    constructor(private mode: string, private principal: string) {
        super();
    }

    execute(): JQueryPromise<boolean> {
        return this.query("/admin/verify-principal", { mode: this.mode, principal: this.principal }, null, (r: verifyResultDto) => r.Valid, null, 25000);//TODO: use endpoints
    }
}

export = verifyPrincipalCommand;

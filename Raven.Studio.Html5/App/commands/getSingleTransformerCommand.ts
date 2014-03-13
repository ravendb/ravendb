/// <reference path="../models/dto.ts" />

import commandBase = require("commands/commandBase");
import database = require("models/database");

class getSingleTransformerCommand extends commandBase{

    constructor(private tranName:string, private db:database) {
        super();
    }

    execute(): JQueryPromise<savedTransformerDto> {
        var getTransformerUrl = "/transformers/" + this.tranName;
        return this.query<savedTransformerDto>(getTransformerUrl, null, this.db);
    }


}

export = getSingleTransformerCommand;
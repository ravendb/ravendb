import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class ChangeNodeVotingModeCommand extends commandBase {


    constructor(private db: database, private connectionInfo: nodeConnectionInfoDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.post("/admin/cluster/changeVotingMode?isVoting=" + !(this.connectionInfo.IsNoneVoter),   //TODO: use endpoints
        ko.toJSON(this.connectionInfo) , 
        this.db,
        { dataType: 'text' })
            .done(() => this.reportSuccess("Node was promoted to voter."))
            .fail((response: JQueryXHR) => this.reportError("Failed to promote node to voter", response.responseText, response.statusText));
    }
}

export = ChangeNodeVotingModeCommand;

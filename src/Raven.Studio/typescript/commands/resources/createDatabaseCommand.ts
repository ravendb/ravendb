import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export type CreateDatabaseDto = Pick<
    Raven.Client.ServerWide.DatabaseRecord,
    "DatabaseName" | "Settings" | "Disabled" | "Encrypted"
> & {
    Topology?: Pick<Raven.Client.ServerWide.DatabaseTopology, "Members" | "DynamicNodesDistribution">;
    Sharding?: {
        Shards: Record<number, { Members?: string[] }>;
        Orchestrator: {
            Topology: Pick<Raven.Client.ServerWide.DatabaseTopology, "Members">
        }
    };
};

export class createDatabaseCommand extends commandBase {
    private databaseDocument: CreateDatabaseDto;
    private replicationFactor: number;

    constructor(databaseDocument: CreateDatabaseDto, replicationFactor: number) {
        super();
        this.replicationFactor = replicationFactor;
        this.databaseDocument = databaseDocument;
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        const args = {
            name: this.databaseDocument.DatabaseName,
            replicationFactor: this.replicationFactor
        };
        const url = endpoints.global.adminDatabases.adminDatabases + this.urlEncodeArgs(args);
        return this.put<Raven.Client.ServerWide.Operations.DatabasePutResult>(url, JSON.stringify(this.databaseDocument), null, { dataType: undefined })
            .done(() => this.reportSuccess(this.databaseDocument.DatabaseName + " created"))
            .fail((response: JQueryXHR) => this.reportError("Failed to create database", response.responseText, response.statusText));
    }
}

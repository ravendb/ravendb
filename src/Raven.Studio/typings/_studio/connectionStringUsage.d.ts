/// <reference path="../tsd.d.ts"/>

// Hand-maintained typings.
//
// `ConnectionString.UsedBy` and the `ConnectionStringUsage` / `ConnectionStringUsageKind` types are
// `internal` in Raven.Client (a server-computed enrichment, not part of the public client API). The
// typings generator only emits public members, so it does not produce them - but the Studio still
// receives them over the wire and needs the shapes. Keep in sync with:
//   src/Raven.Client/Documents/Operations/ConnectionStrings/ConnectionString.cs
//   src/Raven.Client/ServerWide/Operations/ConnectionStrings/ServerWideConnectionString.cs
declare module Raven.Client.Documents.Operations.ConnectionStrings {
    type ConnectionStringUsageKind =
        | "RavenEtl"
        | "SqlEtl"
        | "OlapEtl"
        | "ElasticSearchEtl"
        | "QueueEtl"
        | "SnowflakeEtl"
        | "QueueSink"
        | "CdcSink"
        | "ExternalReplication"
        | "PullReplicationAsSink"
        | "EmbeddingsGeneration"
        | "GenAi"
        | "AiAgent";

    interface ConnectionStringUsage {
        Kind: ConnectionStringUsageKind;
        Id?: number;
        Identifier?: string;
        Name: string;
        // Set only for server-wide connection strings, whose usages are aggregated across databases.
        DatabaseName?: string;
    }

    // Merged (declaration merging) with the generated ConnectionString interface.
    interface ConnectionString {
        UsedBy: Array<ConnectionStringUsage>;
    }
}

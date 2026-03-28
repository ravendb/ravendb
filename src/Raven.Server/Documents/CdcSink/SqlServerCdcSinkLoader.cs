using System;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// CDC Sink loader that dispatches to the correct process implementation
/// based on the SQL connection string's provider factory name.
/// Supports both PostgreSQL (Npgsql) and SQL Server (Microsoft.Data.SqlClient).
/// </summary>
public class DispatchingCdcSinkLoader : CdcSinkLoader
{
    public DispatchingCdcSinkLoader(DocumentDatabase database, ServerStore serverStore)
        : base(database, serverStore)
    {
    }

    protected override CdcSinkProcess CreateProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
    {
        return configuration.Connection.FactoryName switch
        {
            "Npgsql" => new PostgresCdcSinkProcess(configuration, database),
            "System.Data.SqlClient" or "Microsoft.Data.SqlClient" => new SqlServerCdcSinkProcess(configuration, database),
            _ => throw new NotSupportedException(
                $"CDC Sink does not support provider '{configuration.Connection.FactoryName}'. " +
                "Supported providers: Npgsql (PostgreSQL), System.Data.SqlClient / Microsoft.Data.SqlClient (SQL Server).")
        };
    }
}

using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Snowflake;

internal sealed class SnowflakeDocumentTransformer(
    Transformation transformation,
    DocumentDatabase database,
    DocumentsOperationContext context,
    SnowflakeEtlConfiguration config)
    : RelationalDatabaseDocumentTransformerBase<SnowflakeConnectionString, SnowflakeEtlConfiguration>(transformation, database, context, config,
        PatchRequestType.SnowflakeEtl)
{
    protected override List<RelationalDatabaseTableWithRecords> GetEtlTables()
    {
        return _config.SnowflakeTables.Select(RelationalDatabaseTableWithRecords.FromSnowflakeEtlTable).ToList();
    }
}
    

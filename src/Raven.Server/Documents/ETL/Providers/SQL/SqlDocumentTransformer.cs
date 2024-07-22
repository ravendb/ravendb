using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL;

internal sealed class
    SqlDocumentTransformer : RelationalDocumentTransformer<SqlConnectionString, SqlEtlConfiguration>
{
    public SqlDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, SqlEtlConfiguration config) : base(transformation, database, context, config, PatchRequestType.SqlEtl)
    {
    }

    protected override List<RelationalTableWithRecords> GetEtlTables()
    {
        return _config.SqlTables.Select(RelationalTableWithRecords.FromSqlEtlTable).ToList();
    }
}

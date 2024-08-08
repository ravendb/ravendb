using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.SQL;

internal sealed class
    SqlDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, SqlEtlConfiguration config)
    : RelationalDatabaseDocumentTransformerBase<SqlConnectionString, SqlEtlConfiguration>(transformation, database, context, config, PatchRequestType.SqlEtl)
{
    private static readonly JsValue DefaultVarCharSize = 50;

    protected override List<RelationalDatabaseTableWithRecords> GetEtlTables()
    {
        return Config.SqlTables.Select(RelationalDatabaseTableWithRecords.FromSqlEtlTable).ToList();
    }
    
    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);
        
        DocumentScript.ScriptEngine.SetValue("varchar",
            new ClrFunction(DocumentScript.ScriptEngine, "varchar", (value, values) => ToVarcharTranslator(VarcharFunctionCall.AnsiStringType, values)));

        DocumentScript.ScriptEngine.SetValue("nvarchar",
            new ClrFunction(DocumentScript.ScriptEngine, "nvarchar", (value, values) => ToVarcharTranslator(VarcharFunctionCall.StringType, values)));
    }
    
    private JsValue ToVarcharTranslator(JsValue type, JsValue[] args)
    {
        if (args[0].IsString() == false)
            throw new InvalidOperationException("varchar() / nvarchar(): first argument must be a string");

        var sizeSpecified = args.Length > 1;

        if (sizeSpecified && args[1].IsNumber() == false)
            throw new InvalidOperationException("varchar() / nvarchar(): second argument must be a number");

        var item = new JsObject(DocumentScript.ScriptEngine);

        item.FastSetDataProperty(nameof(VarcharFunctionCall.Type), type);
        item.FastSetDataProperty(nameof(VarcharFunctionCall.Value), args[0]);
        item.FastSetDataProperty(nameof(VarcharFunctionCall.Size), sizeSpecified ? args[1] : DefaultVarCharSize);

        return item;
    }

    public sealed class VarcharFunctionCall
    {
        public static JsValue AnsiStringType = DbType.AnsiString.ToString();
        public static JsValue StringType = DbType.String.ToString();

        public DbType Type { get; set; }
        public object Value { get; set; }
        public int Size { get; set; }

        private VarcharFunctionCall()
        {

        }
    }
}

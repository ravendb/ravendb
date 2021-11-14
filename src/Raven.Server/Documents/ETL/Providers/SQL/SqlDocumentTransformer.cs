using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    internal partial class SqlDocumentTransformer : EtlTransformer<ToSqlItem, SqlTableWithRecords, EtlStatsScope, EtlPerformanceOperation>
    {
        private static readonly int DefaultVarCharSize = 50;
        
        private readonly Transformation _transformation;
        private readonly SqlEtlConfiguration _config;
        private readonly Dictionary<string, SqlTableWithRecords> _tables;
        private Dictionary<string, Queue<Attachment>> _loadedAttachments;
        private readonly List<SqlEtlTable> _tablesForScript;

        private EtlStatsScope _stats;

        public SqlDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, SqlEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.SqlEtl), null)
        {
            _transformation = transformation;
            _config = config;

            var destinationTables = transformation.GetCollectionsFromScript();

            LoadToDestinations = destinationTables;

            _tables = new Dictionary<string, SqlTableWithRecords>(destinationTables.Length, StringComparer.OrdinalIgnoreCase);
            _tablesForScript = new List<SqlEtlTable>(destinationTables.Length);

            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _config.SqlTables.Count; i++)
            {
                var table = _config.SqlTables[i];

                if (destinationTables.Contains(table.TableName, StringComparer.OrdinalIgnoreCase))
                    _tablesForScript.Add(table);
            }

            if (_transformation.IsLoadingAttachments)
               _loadedAttachments = new Dictionary<string, Queue<Attachment>>(StringComparer.OrdinalIgnoreCase);
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);
            
            DocumentEngineHandle.SetGlobalClrCallBack("varchar",
                (((value, values) => ToVarcharTranslatorJint(VarcharFunctionCall.AnsiStringType, values)),
                (engine, isConstructCall, self, args) => ToVarcharTranslatorV8(VarcharFunctionCall.AnsiStringType, args))
            );

            DocumentEngineHandle.SetGlobalClrCallBack("nvarchar",
                (((value, values) => ToVarcharTranslatorJint(VarcharFunctionCall.StringType, values)),
                (engine, isConstructCall, self, args) => ToVarcharTranslatorV8(VarcharFunctionCall.StringType, args))
            );
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string tableName, ScriptRunnerResult cols)
        {
            if (tableName == null)
                ThrowLoadParameterIsMandatory(nameof(tableName));

            var result = cols.TranslateToObject(Context);
            var columns = new List<SqlColumn>(result.Count);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < result.Count; i++)
            {
                result.GetPropertyByIndex(i, ref prop);

                var sqlColumn = new SqlColumn
                {
                    Id = prop.Name,
                    Value = prop.Value,
                    Type = prop.Token
                };

                if (_transformation.IsLoadingAttachments && 
                    prop.Token == BlittableJsonToken.String && IsLoadAttachment(prop.Value as LazyStringValue, out var attachmentName))
                {
                    var attachment = _loadedAttachments[attachmentName].Dequeue();

                    sqlColumn.Type = 0;
                    sqlColumn.Value = attachment.Stream;

                    _stats.IncrementBatchSize(attachment.Stream.Length);
                }

                columns.Add(sqlColumn);
            }

            GetOrAdd(tableName).Inserts.Add(new ToSqlItem(Current)
            {
                Columns = columns
            });

            _stats.IncrementBatchSize(result.Size);
        }

        private static unsafe bool IsLoadAttachment(LazyStringValue value, out string attachmentName)
        {
            if (value.Length <= Transformation.AttachmentMarker.Length)
            {
                attachmentName = null;
                return false;
            }

            var buffer = value.Buffer;

            if (*(long*)buffer != 7883660417928814884 || // $attachm
                *(int*)(buffer + 8) != 796159589) // ent/
            {
                attachmentName = null;
                return false;
            }

            attachmentName = value;
            return true;
        }

        private SqlTableWithRecords GetOrAdd(string tableName)
        {
            if (_tables.TryGetValue(tableName, out SqlTableWithRecords table) == false)
            {
                var sqlEtlTable = _config.SqlTables.Find(x => x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));

                if (sqlEtlTable == null)
                    ThrowTableNotDefinedInConfig(tableName);

                _tables[tableName] =
                    table = new SqlTableWithRecords(sqlEtlTable);
            }

            return table;
        }

        private static void ThrowTableNotDefinedInConfig(string tableName)
        {
            throw new InvalidOperationException($"Table '{tableName}' was not defined in the configuration of SQL ETL task");
        }

        public override IEnumerable<SqlTableWithRecords> GetTransformedResults()
        {
            return _tables.Values;
        }

        public override void Transform(ToSqlItem item, EtlStatsScope stats, EtlProcessState state)
        {
            _stats = stats;

            if (item.IsDelete == false)
            {
                Current = item;

                DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
            }

            // ReSharper disable once ForCanBeConvertedToForeach
            for (int i = 0; i < _tablesForScript.Count; i++)
            {
                // delete all the rows that might already exist there

                var sqlTable = _tablesForScript[i];

                if (sqlTable.InsertOnlyMode)
                    continue;

                GetOrAdd(sqlTable.TableName).Deletes.Add(item);
            }
        }
    }
}

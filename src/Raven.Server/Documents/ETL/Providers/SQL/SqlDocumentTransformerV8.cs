using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    //TODO: egor handle duplicate code with V8
    public class SqlDocumentTransformerV8 : EtlTransformerV8<ToSqlItem, SqlTableWithRecords, EtlStatsScope, EtlPerformanceOperation>
    {
        private static readonly int DefaultVarCharSize = 50;
        
        private readonly Transformation _transformation;
        private readonly SqlEtlConfiguration _config;
        private readonly Dictionary<string, SqlTableWithRecords> _tables;
        private readonly Dictionary<string, Queue<Attachment>> _loadedAttachments;
        private readonly List<SqlEtlTable> _tablesForScript;

        private EtlStatsScope _stats;

        public SqlDocumentTransformerV8(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, SqlEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.SqlEtl))
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

            EngineHandle.SetGlobalClrCallBack("varchar", (_, values) => ToVarcharTranslator(EngineHandle.CreateValue(VarcharFunctionCall.AnsiStringType), values));
            EngineHandle.SetGlobalClrCallBack("nvarchar", (_, values) => ToVarcharTranslator(EngineHandle.CreateValue(VarcharFunctionCall.StringType), values));
        }

        private JsHandleV8 ToVarcharTranslator(JsHandleV8 type, JsHandleV8[] args)
        {
            if (args[0].IsStringEx == false)
                throw new InvalidOperationException("varchar() / nvarchar(): first argument must be a string");

            var sizeSpecified = args.Length > 1;

            if (sizeSpecified && args[1].IsNumber == false)
                throw new InvalidOperationException("varchar() / nvarchar(): second argument must be a number");

            var item = EngineHandle.CreateObject();
            item.SetProperty(nameof(VarcharFunctionCall.Type), type, throwOnError: true);
            item.SetProperty(nameof(VarcharFunctionCall.Value), args[0], throwOnError: true);
            item.SetProperty(nameof(VarcharFunctionCall.Size), sizeSpecified ? args[1] : EngineHandle.CreateValue(DefaultVarCharSize), throwOnError: true);

            return item;
        }

        protected override string[] LoadToDestinations { get; }
        protected override void AddLoadedAttachment(JsHandleV8 reference, string name, Attachment attachment)
        {
            var strReference = reference.ToString();
            if (_loadedAttachments.TryGetValue(strReference, out var loadedAttachments) == false)
            {
                loadedAttachments = new Queue<Attachment>();
                _loadedAttachments.Add(strReference, loadedAttachments);
            }

            loadedAttachments.Enqueue(attachment);
        }

        protected override void AddLoadedCounter(JsHandleV8 reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by SQL ETL");
        }

        protected override void AddLoadedTimeSeries(JsHandleV8 reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by SQL ETL");
        }

        protected override void LoadToFunction(string tableName, ScriptRunnerResult<JsHandleV8> cols)
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

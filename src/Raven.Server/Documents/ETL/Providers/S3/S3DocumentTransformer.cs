using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Jint.Native;
using Jint.Runtime;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.S3
{
    internal class S3DocumentTransformer : EtlTransformer<ToS3Item, RowGroup>
    {
        private static readonly JsValue DefaultVarCharSize = 50;
        
        private readonly Transformation _transformation;
        private readonly SqlEtlConfiguration _config;
        private readonly Dictionary<string, RowGroup> _tables;
        private readonly List<SqlEtlTable> _tablesForScript;

        private EtlStatsScope _stats;

        public S3DocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, SqlEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.SqlEtl), null)
        {
            _transformation = transformation;
            _config = config;

            var destinationTables = transformation.GetCollectionsFromScript();

            LoadToDestinations = destinationTables;


            _tables = new Dictionary<string, RowGroup>();
            _tablesForScript = new List<SqlEtlTable>();

            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _config.SqlTables.Count; i++)
            {
                var table = _config.SqlTables[i];

                if (destinationTables.Contains(table.TableName, StringComparer.OrdinalIgnoreCase))
                    _tablesForScript.Add(table);
            }
        }

        protected override bool LoadToS3 => true;


        /*        public override void Initialize(bool debugMode)
                {
                    base.Initialize(debugMode);

        /*            DocumentScript.ScriptEngine.SetValue("varchar",
                        new ClrFunctionInstance(DocumentScript.ScriptEngine, "varchar", (value, values) => ToVarcharTranslator(VarcharFunctionCall.AnsiStringType, values)));

                    DocumentScript.ScriptEngine.SetValue("nvarchar",
                        new ClrFunctionInstance(DocumentScript.ScriptEngine, "nvarchar", (value, values) => ToVarcharTranslator(VarcharFunctionCall.StringType, values)));#1#
                }*/

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string tableName, ScriptRunnerResult res, string key = null)
        {
            if (tableName == null)
                ThrowLoadParameterIsMandatory(nameof(tableName));

            if (key == null) // todo
                ThrowLoadParameterIsMandatory(nameof(tableName));

            var result = res.TranslateToObject(Context);
            var props = new List<SqlColumn>(result.Count);
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

                props.Add(sqlColumn);
            }

            var s3Item = new ToS3Item(Current)
            {
                Properties = props
            };

            var rowGroup = GetOrAdd(tableName, key);

            if (rowGroup.Add(s3Item))
            {
                _stats.IncrementBatchSize(result.Size);
            }

            else
            {
                // todo
            }

        }

        protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
        {
        }

        protected override void AddLoadedCounter(JsValue reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by SQL ETL");
        }

        protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by SQL ETL");
        }

        private RowGroup GetOrAdd(string tableName, string key)
        {
            if (_tables.TryGetValue(tableName, out var table) == false)
            {
                _tables[tableName] = table = new RowGroup(tableName, key);
            }

            return table;
        }

        private static void ThrowTableNotDefinedInConfig(string tableName)
        {
            throw new InvalidOperationException($"Table '{tableName}' was not defined in the configuration of SQL ETL task");
        }

        public override List<RowGroup> GetTransformedResults()
        {
            return _tables.Values.ToList();
        }

        public override void Transform(ToS3Item item, EtlStatsScope stats, EtlProcessState state)
        {
            _stats = stats;
            if (item.IsDelete)
                return;

            Current = item;
            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
        }

/*        private JsValue ToVarcharTranslator(JsValue type, JsValue[] args)
        {
            if (args[0].IsString() == false)
                throw new InvalidOperationException("varchar() / nvarchar(): first argument must be a string");

            var sizeSpecified = args.Length > 1;

            if (sizeSpecified && args[1].IsNumber() == false)
                throw new InvalidOperationException("varchar() / nvarchar(): second argument must be a number");

            var item = DocumentScript.ScriptEngine.Object.Construct(Arguments.Empty);

            item.Set(nameof(VarcharFunctionCall.Type), type, true);
            item.Set(nameof(VarcharFunctionCall.Value), args[0], true);
            item.Set(nameof(VarcharFunctionCall.Size), sizeSpecified ? args[1] : DefaultVarCharSize, true);

            return item;
        }

        public class VarcharFunctionCall
        {
            public static JsValue AnsiStringType = DbType.AnsiString.ToString();
            public static JsValue StringType = DbType.String.ToString();

            public DbType Type { get; set; }
            public object Value { get; set; }
            public int Size { get; set; }

            private VarcharFunctionCall()
            {

            }
        }*/
    }
}

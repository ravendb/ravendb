using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Parquet
{
    internal class ParquetDocumentTransformer : EtlTransformer<ToParquetItem, RowGroups>
    {
        private readonly ParquetEtlConfiguration _config;
        private readonly Dictionary<string, RowGroups> _tables;

        private EtlStatsScope _stats;

        private const string DateFormat = "yyyy-MM-dd-HH-mm";

        public ParquetDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, ParquetEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.SqlEtl), null)
        {
            _config = config;
            _tables = new Dictionary<string, RowGroups>();
            LoadToDestinations = transformation.GetCollectionsFromScript();
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);

            foreach (var collection in LoadToDestinations)
            {
                var name = Transformation.LoadTo + collection;
                DocumentScript.ScriptEngine.SetValue(name, new ClrFunctionInstance(DocumentScript.ScriptEngine, name,
                    (value, args) => LoadToS3FunctionTranslator(collection, value, args)));
            }
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject)
        {
        }

        private void LoadToFunction(string tableName, ScriptRunnerResult res, string key)
        {
            if (tableName == null)
                ThrowLoadParameterIsMandatory(nameof(tableName));

            if (key == null)
                ThrowLoadParameterIsMandatory(nameof(key));

            var result = res.TranslateToObject(Context);
            var props = new List<SqlColumn>(result.Count);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < result.Count; i++)
            {
                result.GetPropertyByIndex(i, ref prop);
                props.Add(new SqlColumn
                {
                    Id = prop.Name,
                    Value = prop.Value,
                    Type = prop.Token
                });
            }

            var s3Item = new ToParquetItem(Current)
            {
                Properties = props
            };

            var rowGroups = GetOrAdd(tableName, key);
            rowGroups.Add(s3Item);

            _stats.IncrementBatchSize(result.Size);
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

        private RowGroups GetOrAdd(string tableName, string key)
        {
            var name = tableName + "_" + key;

            if (_tables.TryGetValue(name, out var table) == false)
            {
                _tables[name] = table = new RowGroups(tableName, key);
            }

            return table;
        }


        private JsValue LoadToS3FunctionTranslator(string name, JsValue self, JsValue[] args)
        {
            if (args.Length != 2)
                ThrowInvalidScriptMethodCall($"loadTo{name}(key, obj) must be called with exactly 2 parameters");

            if (args[0].IsDate() == false)
                ThrowInvalidScriptMethodCall($"loadTo{name}(key, obj) argument 'key' must be a date object");
            
            if (args[1].IsObject() == false)
                ThrowInvalidScriptMethodCall($"loadTo{name}(key, obj) argument 'obj' must be an object");

            var key = args[0].AsDate().ToDateTime().ToString(DateFormat);
            var result = new ScriptRunnerResult(DocumentScript, args[1].AsObject());
            LoadToFunction(name, result, key);

            return result.Instance;
        }


        public override List<RowGroups> GetTransformedResults()  
        {
            return _tables.Values.ToList();
        }

        public override void Transform(ToParquetItem item, EtlStatsScope stats, EtlProcessState state)
        {
            _stats = stats;
            if (item.IsDelete)
                return;

            Current = item;
            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
        }
    }
}

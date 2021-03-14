using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    internal class OlapDocumentTransformer : EtlTransformer<ToOlapItem, OlapTransformedItems>
    {
        private readonly OlapEtlConfiguration _config;
        private readonly Dictionary<string, OlapTransformedItems> _tables;

        private EtlStatsScope _stats;

        private const string DateFormat = "yyyy-MM-dd-HH-mm";
        private string _tmpFilePath, _fileNamePrefix;
        private Logger _logger;


        public OlapDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, OlapEtlConfiguration config, string processName, Logger logger)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.OlapEtl), null)
        {
            _config = config;
            _tables = new Dictionary<string, OlapTransformedItems>();
            _logger = logger;
            LoadToDestinations = transformation.GetCollectionsFromScript();

            var localSettings = BackupTask.GetBackupConfigurationFromScript(_config.Connection.LocalSettings, x => JsonDeserializationServer.LocalSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false);

            _tmpFilePath = localSettings?.FolderPath ??
                           (database.Configuration.Storage.TempPath ?? database.Configuration.Core.DataDirectory).FullPath;

            _fileNamePrefix = $"{Database.Name}_{processName}";
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);

            foreach (var table in LoadToDestinations)
            {
                var name = Transformation.LoadTo + table;
                DocumentScript.ScriptEngine.SetValue(name, new ClrFunctionInstance(DocumentScript.ScriptEngine, name,
                    (value, args) => LoadToS3FunctionTranslator(table, value, args)));
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

            var olapItem = new ToOlapItem(Current)
            {
                Properties = props
            };

            var transformed = GetOrAdd(tableName, key);
            transformed.AddItem(olapItem);

            _stats.IncrementBatchSize(result.Size);
        }

        protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
        {
        }

        protected override void AddLoadedCounter(JsValue reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by OLAP ETL");
        }

        protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by OLAP ETL");
        }

        private OlapTransformedItems GetOrAdd(string tableName, string key)
        {
            var name = tableName + "_" + key;

            if (_tables.TryGetValue(name, out var table) == false)
            {
                _tables[name] = _config.Format switch
                {
                    OlapEtlFileFormat.Parquet => (table = new ParquetTransformedItems(tableName, key, _tmpFilePath, _fileNamePrefix, _config, _logger)),
                    _ => throw new ArgumentOutOfRangeException(nameof(OlapEtlConfiguration.Format))
                };
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

            var key = args[0].AsDate().ToDateTime().ToString(DateFormat); //todo
            var result = new ScriptRunnerResult(DocumentScript, args[1].AsObject());
            LoadToFunction(name, result, key);

            return result.Instance;
        }


        public override IEnumerable<OlapTransformedItems> GetTransformedResults()  
        {
            return _tables.Values;
        }

        public override void Transform(ToOlapItem item, EtlStatsScope stats, EtlProcessState state)
        {
            _stats = stats;
            if (item.IsDelete)
                return;

            Current = item;
            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
        }
    }
}

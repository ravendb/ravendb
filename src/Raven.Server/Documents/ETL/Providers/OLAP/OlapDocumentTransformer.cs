using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
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
    internal class OlapDocumentTransformer : EtlTransformer<ToOlapItem, OlapTransformedItems, OlapEtlStatsScope, OlapEtlPerformanceOperation>
    {
        private const string DateFormat = "yyyy-MM-dd-HH-mm";
        private readonly OlapEtlConfiguration _config;
        private readonly Dictionary<string, OlapTransformedItems> _tables;
        private readonly string _fileNameSuffix, _tmpFilePath;
        private OlapEtlStatsScope _stats;
        private readonly Logger _logger;

        private ObjectInstance _noPartition;
        private const string PartitionKeys = "$partition_keys";
        private const string DefaultPartitionColumnName = "_partition";
        private const string CustomFieldName = "$custom_field";

        public OlapDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, OlapEtlConfiguration config, Logger logger)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.OlapEtl), null)
        {
            _config = config;
            _tables = new Dictionary<string, OlapTransformedItems>();
            _logger = logger;

            var localSettings = BackupTask.GetBackupConfigurationFromScript(_config.Connection.LocalSettings, x => JsonDeserializationServer.LocalSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false);

            _tmpFilePath = localSettings?.FolderPath ??
                           (database.Configuration.Storage.TempPath ?? database.Configuration.Core.DataDirectory).FullPath;

            _fileNameSuffix = OlapEtl.EnsureSafeName($"{database.Name}-{_config.Name}-{transformation.Name}", isFolderPath: false);

            LoadToDestinations = transformation.GetCollectionsFromScript();
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);

            DocumentScript.ScriptEngine.SetValue(Transformation.LoadTo, new ClrFunctionInstance(DocumentScript.ScriptEngine, Transformation.LoadTo, LoadToFunctionTranslator));

            foreach (var table in LoadToDestinations)
            {
                var name = Transformation.LoadTo + table;
                DocumentScript.ScriptEngine.SetValue(name, new ClrFunctionInstance(DocumentScript.ScriptEngine, name,
                    (self, args) => LoadToFunctionTranslator(table, args)));
            }

            DocumentScript.ScriptEngine.SetValue("partitionBy", new ClrFunctionInstance(DocumentScript.ScriptEngine, "partitionBy", PartitionBy));
            DocumentScript.ScriptEngine.SetValue("noPartition", new ClrFunctionInstance(DocumentScript.ScriptEngine, "noPartition", NoPartition));

            if (_config.CustomField != null)
            {
                DocumentScript.ScriptEngine.SetValue(CustomFieldName, new JsString(_config.CustomField));
            }
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject)
        {
        }

        private void LoadToFunction(string tableName, string key, ScriptRunnerResult runnerResult)
        {
            if (key == null)
                ThrowLoadParameterIsMandatory(nameof(key));

            var result = runnerResult.TranslateToObject(Context);
            var props = new List<OlapColumn>(result.Count);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < result.Count; i++)
            {
                result.GetPropertyByIndex(i, ref prop);
                props.Add(new OlapColumn
                {
                    Name = prop.Name,
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
            throw new NotSupportedException("Attachments aren't supported by OLAP ETL");
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
            var name = key;

            if (_tables.TryGetValue(name, out var table) == false)
            {
                _tables[name] = _config.Format switch
                {
                    OlapEtlFileFormat.Parquet => (table = new ParquetTransformedItems(tableName, key, _tmpFilePath, _fileNameSuffix, _config, _logger)),
                    _ => throw new ArgumentOutOfRangeException(nameof(OlapEtlConfiguration.Format))
                };
            }

            return table;
        }

        private JsValue LoadToFunctionTranslator(JsValue self, JsValue[] args)
        {
            var methodSignature = "loadTo(name, key, obj)";

            if (args.Length != 3)
                ThrowInvalidScriptMethodCall($"{methodSignature} must be called with exactly 3 parameters");

            if (args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");

            if (args[1].IsObject() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be an object");

            if (args[2].IsObject() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} third argument must be an object");

            return LoadToFunctionTranslatorInternal(args[0].AsString(), args[1].AsObject(), args[2].AsObject(), methodSignature);
        }

        private JsValue LoadToFunctionTranslator(string name, JsValue[] args)
        {
            var methodSignature = $"loadTo{name}(key, obj)";

            if (args.Length != 2)
                ThrowInvalidScriptMethodCall($"{methodSignature} must be called with exactly 2 parameters");

            if (args[1].IsObject() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'obj' must be an object");

            if (args[0].IsObject() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'key' must be an object");

            return LoadToFunctionTranslatorInternal(name, args[0].AsObject(), args[1].AsObject(), methodSignature);
        }

        private JsValue LoadToFunctionTranslatorInternal(string name, ObjectInstance key, ObjectInstance obj, string methodSignature)
        {
            var objectInstance = key;
            if (objectInstance.HasOwnProperty(PartitionKeys) == false)
                ThrowInvalidScriptMethodCall(
                    $"{methodSignature} argument 'key' must have {PartitionKeys} property. Did you forget to use 'partitionBy(p)' / 'noPartition()' ? ");

            var partitionBy = objectInstance.GetOwnProperty(PartitionKeys).Value;
            var result = new ScriptRunnerResult(DocumentScript, obj);

            if (partitionBy.IsNull())
            {
                // no partition
                LoadToFunction(name, key: name, result);
                return result.Instance;
            }

            if (partitionBy.IsArray() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} property {PartitionKeys} of argument 'key' must be an array instance");

            StringBuilder sb = new StringBuilder(name);
            var arr = partitionBy.AsArray();
            foreach (var item in arr)
            {
                if (item.IsArray() == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} items in array {PartitionKeys} of argument 'key' must be array instances");

                var tuple = item.AsArray();
                if (tuple.Length != 2)
                    ThrowInvalidScriptMethodCall(
                        $"{methodSignature} items in array {PartitionKeys} of argument 'key' must be array instances of size 2, but got '{tuple.Length}'");

                sb.Append('/').Append(tuple[0]).Append('=');
                var val = tuple[1].IsDate()
                    ? tuple[1].AsDate().ToDateTime().ToString(DateFormat)
                    : tuple[1];

                sb.Append(val);
            }

            LoadToFunction(name, sb.ToString(), result);
            return result.Instance;
        }

        private JsValue PartitionBy(JsValue self, JsValue[] args)
        {
            if (args.Length == 0)
                ThrowInvalidScriptMethodCall("partitionBy(args) cannot be called with 0 arguments");

            JsValue array;
            if (args.Length == 1 && args[0].IsArray() == false)
            {
                array = JsValue.FromObject(DocumentScript.ScriptEngine, new[]
                {
                    JsValue.FromObject(DocumentScript.ScriptEngine, new[]
                    {
                        new JsString(DefaultPartitionColumnName), args[0]
                    })
                });
            }
            else
            {
                array = JsValue.FromObject(DocumentScript.ScriptEngine, args);
            }

            var o = new ObjectInstance(DocumentScript.ScriptEngine);
            o.FastAddProperty(PartitionKeys, array, false, true, false);

            return o;
        }

        private JsValue NoPartition(JsValue self, JsValue[] args)
        {
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("noPartition() must be called with 0 parameters");

            if (_noPartition == null)
            {
                _noPartition = new ObjectInstance(DocumentScript.ScriptEngine);
                _noPartition.FastAddProperty(PartitionKeys, JsValue.Null, false, true, false);
            }

            return _noPartition;
        }

        public override IEnumerable<OlapTransformedItems> GetTransformedResults() => _tables.Values;

        public override void Transform(ToOlapItem item, OlapEtlStatsScope stats, EtlProcessState state)
        {
            // Tombstones extraction is skipped by OLAP ETL. This must never happen
            Debug.Assert(item.IsDelete == false,
                $"Invalid item '{item.DocumentId}', OLAP ETL shouldn't handle tombstones");

            _stats = stats;
            Current = item;
            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
        }
    }
}

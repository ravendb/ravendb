using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class OlapDocumentTransformerJint : EtlTransformerJint<ToOlapItem, OlapTransformedItems, OlapEtlStatsScope, OlapEtlPerformanceOperation>
    {
        private const string DateFormat = "yyyy-MM-dd-HH-mm";
        private readonly OlapEtlConfiguration _config;
        private readonly Dictionary<string, OlapTransformedItems> _tables;
        private readonly string _fileNameSuffix, _localFilePath;
        private OlapEtlStatsScope _stats;

        private const string PartitionKeys = "$partition_keys";
        private const string DefaultPartitionColumnName = "_partition";
        private const string CustomPartition = "$customPartitionValue";

        private JsHandleJint _noPartition;

        public OlapDocumentTransformerJint(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, OlapEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.OlapEtl))
        {
            _config = config;
            _tables = new Dictionary<string, OlapTransformedItems>();

            var localSettings = BackupTask.GetBackupConfigurationFromScript(_config.Connection.LocalSettings, x => JsonDeserializationServer.LocalSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false);

            _localFilePath = localSettings?.FolderPath ??
                           (database.Configuration.Storage.TempPath ?? database.Configuration.Core.DataDirectory).FullPath;

            _fileNameSuffix = ParquetTransformedItems.GetSafeNameForRemoteDestination($"{database.Name}-{_config.Name}-{transformation.Name}");

            LoadToDestinations = transformation.GetCollectionsFromScript();
        }

        public override void Dispose()
        {
            _noPartition.Dispose();
            base.Dispose();
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);

            EngineHandle.SetGlobalClrCallBack(Transformation.LoadTo, LoadToFunctionTranslator);

            foreach (var table in LoadToDestinations)
            {
                EngineHandle.SetGlobalClrCallBack($"{Transformation.LoadTo}{table}", (_, args) => LoadToFunctionTranslator(table, args));
            }

            EngineHandle.SetGlobalClrCallBack("partitionBy", PartitionBy);
            EngineHandle.SetGlobalClrCallBack("noPartition", NoPartition);

            var customPartitionValue = _config.CustomPartitionValue != null
                ? EngineHandle.CreateValue(_config.CustomPartitionValue)
                : JsHandleJint.Empty;

            EngineHandle.SetGlobalProperty(CustomPartition, customPartitionValue);
        }

        protected override string[] LoadToDestinations { get; }
        protected override void AddLoadedAttachment(JsHandleJint reference, string name, Attachment attachment)
        {
            throw new NotSupportedException("Attachments aren't supported by OLAP ETL");
        }

        protected override void AddLoadedCounter(JsHandleJint reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by OLAP ETL");
        }

        protected override void AddLoadedTimeSeries(JsHandleJint reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by OLAP ETL");
        }

        protected override void LoadToFunction(string tableName, ScriptRunnerResult<JsHandleJint> colsAsObject)
        {
        }

        private void LoadToFunction(string tableName, string key, ScriptRunnerResult<JsHandleJint> runnerResult, List<string> partitions = null)
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

            var transformed = GetOrAdd(tableName, key, partitions);
            transformed.AddItem(olapItem);

            _stats.IncrementBatchSize(result.Size);
        }

        private OlapTransformedItems GetOrAdd(string tableName, string key, List<string> partitions)
        {
            var name = key;

            if (_tables.TryGetValue(name, out var table) == false)
            {
                _tables[name] = _config.Format switch
                {
                    OlapEtlFileFormat.Parquet => (table = new ParquetTransformedItems(tableName, key, _localFilePath, _fileNameSuffix, partitions, _config)),
                    _ => throw new ArgumentOutOfRangeException(nameof(OlapEtlConfiguration.Format))
                };
            }

            return table;
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

        private JsHandleJint LoadToFunctionTranslator(JsHandleJint self, JsHandleJint[] args)
        {
            var methodSignature = "loadTo(name, key, obj)";

            if (args.Length != 3)
                ThrowInvalidScriptMethodCall($"{methodSignature} must be called with exactly 3 parameters");

            if (args[0].IsStringEx == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");

            if (args[1].IsObject == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be an object");

            if (args[2].IsObject == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} third argument must be an object");

            return LoadToFunctionTranslatorInternal(args[0].AsString, args[1], args[2], methodSignature);
        }

        private JsHandleJint LoadToFunctionTranslator(string name, JsHandleJint[] args)
        {
            var methodSignature = $"loadTo{name}(key, obj)";

            if (args.Length != 2)
                ThrowInvalidScriptMethodCall($"{methodSignature} must be called with exactly 2 parameters");

            if (args[1].IsObject == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'obj' must be an object");

            if (args[0].IsObject == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'key' must be an object");

            return LoadToFunctionTranslatorInternal(name, args[0], args[1], methodSignature);
        }

        private JsHandleJint LoadToFunctionTranslatorInternal(string name, JsHandleJint key, JsHandleJint obj, string methodSignature)
        {
            if (key.HasOwnProperty(PartitionKeys) == false)
                ThrowInvalidScriptMethodCall(
                    $"{methodSignature} argument 'key' must have {PartitionKeys} property. Did you forget to use 'partitionBy(p)' / 'noPartition()' ? ");

            var partitionBy = key.GetOwnProperty(PartitionKeys);
            var result = new ScriptRunnerResultJint(DocumentScript, obj);

            if (partitionBy.IsNull)
            {
                // no partition
                LoadToFunction(name, key: name, result);
                return result.Instance;
            }

            if (partitionBy.IsArray == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} property {PartitionKeys} of argument 'key' must be an array instance");

            var sb = new StringBuilder(name);
            int arrayLength = partitionBy.ArrayLength;
            var partitions = new List<string>(arrayLength);
            for (int i = 0; i < arrayLength; i++)
            {
                using (var item = partitionBy.GetProperty(i))
                {
                    if (item.IsArray == false)
                        ThrowInvalidScriptMethodCall($"{methodSignature} items in array {PartitionKeys} of argument 'key' must be array instances");

                    if (item.ArrayLength != 2)
                        ThrowInvalidScriptMethodCall(
                            $"{methodSignature} items in array {PartitionKeys} of argument 'key' must be array instances of size 2, but got '{item.ArrayLength}'");

                    sb.Append('/');
                    using (var tuple1 = item.GetProperty(1))
                    {
                        string val = tuple1.IsDate
                            ? tuple1.AsDate.ToString(DateFormat)
                            : tuple1.ToString();
                        using (var tuple0 = item.GetProperty(0))
                        {
                            var partition = $"{tuple0}={val}";
                            sb.Append(partition);
                            partitions.Add(partition);
                        }
                    }
                }
            }

            LoadToFunction(name, sb.ToString(), result, partitions);
            return result.Instance;
        }

        private JsHandleJint PartitionBy(JsHandleJint self, JsHandleJint[] args)
        {
            if (args.Length == 0)
                ThrowInvalidScriptMethodCall("partitionBy(args) cannot be called with 0 arguments");
            JsHandleJint jsArr;
            if (args.Length == 1 && args[0].IsArray == false)
            {
                jsArr = EngineHandle.CreateArray(new[]
                {
                    EngineHandle.CreateArray(new[]
                    {
                        EngineHandle.CreateValue(DefaultPartitionColumnName), args[0]
                    })
                });
            }
            else
            {
                jsArr = EngineHandle.CreateArray(args);

                //TODO: egor need to care about places that we use FromObjectGen, because passing there the args will create a new object with ObjectWrapper's even if it the objects were arrays
                //TODO: because it expects jsvalue but we pass jsHandleJint so it jsut process it as object.
                //jsArr = EngineHandle.FromObjectGen(args);
            }

            JsHandleJint o;
            /*using (jsArr)
            {*/
                o = EngineHandle.CreateObject();
                o.FastAddProperty(PartitionKeys, jsArr, false, true, false);
            /*}*/
            return o;
        }

        private JsHandleJint NoPartition(JsHandleJint self, JsHandleJint[] args)
        {
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("noPartition() must be called with 0 parameters");

            if (_noPartition.IsEmpty)
            {
                _noPartition = EngineHandle.CreateObject();
                _noPartition.FastAddProperty(PartitionKeys, EngineHandle.Null, writable: false, enumerable: true, configurable: false);
            }

            return _noPartition;
        }
    }
}

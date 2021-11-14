using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    internal partial class OlapDocumentTransformer : EtlTransformer<ToOlapItem, OlapTransformedItems, OlapEtlStatsScope, OlapEtlPerformanceOperation>
    {
        private const string DateFormat = "yyyy-MM-dd-HH-mm";
        private readonly OlapEtlConfiguration _config;
        private readonly Dictionary<string, OlapTransformedItems> _tables;
        private readonly string _fileNameSuffix, _localFilePath;
        private OlapEtlStatsScope _stats;

        private const string PartitionKeys = "$partition_keys";
        private const string DefaultPartitionColumnName = "_partition";
        private const string CustomPartition = "$customPartitionValue";
        
        private JsHandle _noPartition;

        public OlapDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, OlapEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.OlapEtl), null)
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
            
            DocumentEngineHandle.SetGlobalClrCallBack(Transformation.LoadTo, (LoadToFunctionTranslatorJint, LoadToFunctionTranslatorV8));

            foreach (var table in LoadToDestinations)
            {
                var name = Transformation.LoadTo + table;
                DocumentEngineHandle.SetGlobalClrCallBack(name,
                    (((self, args) => LoadToFunctionTranslatorJint(table, args)),
                    (engine, isConstructCall, self, args) => LoadToFunctionTranslatorV8(engine, table, args))
                );
            }

            DocumentEngineHandle.SetGlobalClrCallBack("partitionBy", (PartitionByJint, PartitionByV8));
            DocumentEngineHandle.SetGlobalClrCallBack("noPartition", (NoPartitionJint, NoPartitionV8));

            var customPartitionValue = _config.CustomPartitionValue != null
                ? DocumentEngineHandle.CreateValue(_config.CustomPartitionValue)
                : JsHandle.Empty;

            DocumentEngineHandle.SetGlobalProperty(CustomPartition, customPartitionValue);
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject)
        {
        }

        private void LoadToFunction(string tableName, string key, ScriptRunnerResult runnerResult, List<string> partitions = null)
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
    }
}

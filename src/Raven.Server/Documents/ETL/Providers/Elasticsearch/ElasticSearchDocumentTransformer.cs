using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    internal class ElasticSearchDocumentTransformer : EtlTransformer<ElasticSearchItem, ElasticSearchIndexWithRecords, EtlStatsScope, EtlPerformanceOperation>
    {
        private readonly ElasticSearchEtlConfiguration _config;
        private readonly Dictionary<string, ElasticSearchIndexWithRecords> _indexes;
        private readonly List<ElasticSearchIndex> _indexesForScript;

        public ElasticSearchDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, ElasticSearchEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.ElasticSearchEtl), null)
        {
            _config = config;

            var destinationIndexes = transformation.GetCollectionsFromScript();

            LoadToDestinations = destinationIndexes;

            _indexes = new Dictionary<string, ElasticSearchIndexWithRecords>(destinationIndexes.Length, StringComparer.OrdinalIgnoreCase);
            _indexesForScript = new List<ElasticSearchIndex>(destinationIndexes.Length);

            for (var i = 0; i < _config.ElasticIndexes.Count; i++)
            {
                var table = _config.ElasticIndexes[i];

                if (destinationIndexes.Contains(table.IndexName, StringComparer.OrdinalIgnoreCase))
                    _indexesForScript.Add(table);
            }
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);

            if (DocumentScript == null)
                return;
        }

        protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
        {
            throw new NotSupportedException("Attachments aren't supported by ElasticSearch ETL");
        }

        protected override void AddLoadedCounter(JsValue reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by ElasticSearch ETL");
        }

        protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by ElasticSearch ETL");
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string indexName, ScriptRunnerResult document)
        {
            if (indexName == null)
                ThrowLoadParameterIsMandatory(nameof(indexName));

            var result = document.TranslateToObject(Context);


            var index = GetOrAdd(indexName);

            if (result.TryGet(index.DocumentIdProperty, out object _) == false)
            {
                result.Modifications = new DynamicJsonValue
                {
                    [index.DocumentIdProperty] = ElasticSearchEtl.LowerCaseDocumentIdProperty(Current.Document.Id)
                };
            }

            index.Inserts.Add(new ElasticSearchItem(Current) {TransformationResult = result});
        }

        public override List<ElasticSearchIndexWithRecords> GetTransformedResults()
        {
            return _indexes.Values.ToList();
        }

        public override void Transform(ElasticSearchItem item, EtlStatsScope stats, EtlProcessState state)
        {
            if (item.IsDelete == false)
            {
                Current = item;
                DocumentScript.Run(Context, Context, "execute", new object[] {Current.Document}).Dispose();
            }

            for (int i = 0; i < _indexesForScript.Count; i++)
            {
                // delete all the rows that might already exist there
                var elasticIndex = _indexesForScript[i];

                GetOrAdd(elasticIndex.IndexName).Deletes.Add(item);
            }
        }

        private ElasticSearchIndexWithRecords GetOrAdd(string indexName)
        {
            if (_indexes.TryGetValue(indexName, out ElasticSearchIndexWithRecords index) == false)
            {
                var elasticIndex = _config.ElasticIndexes.Find(x => x.IndexName.Equals(indexName, StringComparison.OrdinalIgnoreCase));

                if (elasticIndex == null)
                    ThrowIndexNotDefinedInConfig(indexName);

                _indexes[indexName] = index = new ElasticSearchIndexWithRecords(elasticIndex);
            }

            return index;
        }

        private static void ThrowIndexNotDefinedInConfig(string indexName)
        {
            throw new InvalidOperationException($"Index '{indexName}' was not defined in the configuration of ElasticSearch ETL task");
        }
    }
}

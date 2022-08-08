using Raven.Client;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries.Jint
{
    public sealed class CountersAndTimeSeriesJsLuceneDocumentConverterJint : LuceneJavascriptDocumentConverterBase<JsHandleJint>
    {
        private JintEngineEx _engineEx => EngineHandle as JintEngineEx;

        public CountersAndTimeSeriesJsLuceneDocumentConverterJint(MapTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterJint(MapCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterJint(MapReduceTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterJint(MapReduceCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }
        protected override object GetBlittableSupportedType(JsHandleJint val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext)
        {
            return TypeConverter.ToBlittableSupportedType(val, _engineEx, flattenArrays: false, forIndexing: true, indexContext);
        }
    }
}

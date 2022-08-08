using Raven.Client;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.TimeSeries.V8
{
    public sealed class CountersAndTimeSeriesJsLuceneDocumentConverterV8 : LuceneJavascriptDocumentConverterBase<JsHandleV8>
    {
        private V8EngineEx _engineEx => EngineHandle as V8EngineEx;
        public CountersAndTimeSeriesJsLuceneDocumentConverterV8(MapTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterV8(MapCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterV8(MapReduceTimeSeriesIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        public CountersAndTimeSeriesJsLuceneDocumentConverterV8(MapReduceCountersIndex index)
            : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName)
        {
        }

        protected override object GetBlittableSupportedType(JsHandleV8 val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext)
        {
            return TypeConverter.ToBlittableSupportedType(val, _engineEx, flattenArrays: false, forIndexing: true, indexContext);
        }
    }
}

using Raven.Client;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CountersAndTimeSeriesJintCoraxDocumentConverter : CoraxJintDocumentConverterBase
{
    public CountersAndTimeSeriesJintCoraxDocumentConverter(MapTimeSeriesIndex index) : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName, canContainSourceDocumentId: true)
    {
    }

    public CountersAndTimeSeriesJintCoraxDocumentConverter(MapCountersIndex index) : base(index, index.Definition.IndexDefinition, numberOfBaseFields: 3, keyFieldName: Constants.Documents.Indexing.Fields.DocumentIdFieldName, storeValue: true, storeValueFieldName: Constants.Documents.Indexing.Fields.ValueFieldName, canContainSourceDocumentId: true)
    {
    }
}

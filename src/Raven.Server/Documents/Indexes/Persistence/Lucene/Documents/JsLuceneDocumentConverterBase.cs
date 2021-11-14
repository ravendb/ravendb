using Raven.Client;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public abstract class JsLuceneDocumentConverterBase : LuceneDocumentConverterBase
    {
        protected readonly IndexFieldOptions _allFields;

        protected JsLuceneDocumentConverterBase(Index index, IndexDefinition indexDefinition, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            : base(index, index.Configuration.IndexEmptyEntries, numberOfBaseFields, keyFieldName, storeValue, storeValueFieldName)
        {
            indexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
        }

        protected const string ValuePropertyName = "$value";
        protected const string OptionsPropertyName = "$options";
        protected const string NamePropertyName = "$name";
        protected const string SpatialPropertyName = "$spatial";
        protected const string BoostPropertyName = "$boost";
    }
}

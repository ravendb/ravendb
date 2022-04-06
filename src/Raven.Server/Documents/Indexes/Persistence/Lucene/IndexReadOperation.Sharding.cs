using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public partial class IndexReadOperation
{
    partial void AddOrderByFields(IndexQueryServerSide query, global::Lucene.Net.Documents.Document document, int doc, ref Document d)
    {
        // * for sharded queries, we'll send the order by fields separately
        // * for a map-reduce index, it's fields are the ones that are used for sorting
        if (_index.DocumentDatabase is ShardedDocumentDatabase == false || query.Metadata.OrderBy?.Length > 0 == false || _indexType.IsMapReduce())
            return;

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "review after Corax is merged");

        var documentWithOrderByFields = DocumentWithOrderByFields.From(d);

        foreach (var field in query.Metadata.OrderBy)
        {
            switch (field.OrderingType)
            {
                case OrderByFieldType.Long:
                    documentWithOrderByFields.AddLongOrderByField(_searcher.IndexReader.GetLongValueFor(field.LuceneOrderByName, FieldCache_Fields.NUMERIC_UTILS_LONG_PARSER, doc, _state));
                    break;
                case OrderByFieldType.Double:
                    documentWithOrderByFields.AddDoubleOrderByField(_searcher.IndexReader.GetDoubleValueFor(field.LuceneOrderByName, FieldCache_Fields.NUMERIC_UTILS_DOUBLE_PARSER, doc, _state));
                    break;
                default:
                    documentWithOrderByFields.AddStringOrderByField(_searcher.IndexReader.GetStringValueFor(field.LuceneOrderByName, doc, _state));
                    break;
            }
        }

        d = documentWithOrderByFields;
    }
}

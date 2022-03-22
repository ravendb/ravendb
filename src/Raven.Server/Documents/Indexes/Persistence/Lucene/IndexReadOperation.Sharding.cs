using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public partial class IndexReadOperation
{
    partial void AddOrderByFields(IndexQueryServerSide query, global::Lucene.Net.Documents.Document document, ref Document d)
    {
        // * for sharded queries, we'll send the order by fields separately
        // * for a map-reduce index, it's fields are the ones that are used for sorting
        if (_index.DocumentDatabase is ShardedDocumentDatabase == false || query.Metadata.OrderBy?.Length > 0 == false || _indexType.IsMapReduce())
            return;

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "review after Corax is merged");

        var documentWithOrderByFields = DocumentWithOrderByFields.From(d);

        foreach (var field in query.Metadata.OrderBy)
        {
            // https://issues.hibernatingrhinos.com/issue/RavenDB-17888
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
                "make it work without storing the fields in the index");

            var fieldValue = document.GetField(field.Name.Value).StringValue(_state);
            documentWithOrderByFields.AddOrderByField(field.Name.Value, fieldValue);
        }

        d = documentWithOrderByFields;
    }
}

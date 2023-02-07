using System.Text;
using Corax;
using Corax.Queries;
using Corax.Utils;
using Corax.Utils.Spatial;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Sharding;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public partial class CoraxIndexReadOperation
{
    private SortingMatch.SpatialAscendingMatchComparer? _ascSpatialComparer;
    private SortingMatch.SpatialDescendingMatchComparer? _descSpatialComparer;

    partial void AddOrderByFields(IndexQueryServerSide query, long indexEntryId, OrderMetadata[] orderByFields, ref Document d)
    {
        var documentWithOrderByFields = DocumentWithOrderByFields.From(d);

        for (int i = 0; i < query.Metadata.OrderBy.Length; i++)
        {
            var orderByField = query.Metadata.OrderBy[i];

            if (orderByField.OrderingType == OrderByFieldType.Random)
                break; // we order by random when merging results from shards

            if (orderByField.OrderingType == OrderByFieldType.Score)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "RavenDB-13927 Order by score");
                throw new NotSupportedInShardingException("Ordering by score is not supported in sharding");
            }

            var orderByFieldMetadata = orderByFields[i];

            IndexEntryReader entryReader = _indexSearcher.GetReaderAndIdentifyFor(indexEntryId, out _);
            IndexEntryReader.FieldReader reader = entryReader.GetFieldReaderFor(orderByFieldMetadata.Field);

            switch (orderByField.OrderingType)
            {
                case OrderByFieldType.Long:
                    reader.Read<long>(out var longValue);
                    documentWithOrderByFields.AddLongOrderByField(longValue);
                    break;
                case OrderByFieldType.Double:
                    reader.Read<double>(out var doubleValue);
                    documentWithOrderByFields.AddDoubleOrderByField(doubleValue);
                    break;
                case OrderByFieldType.Distance:
                {
                    reader.Read(out (double lat, double lon) coordinates);

                    ISpatialComparer comparer = orderByField.Ascending
                        ? _ascSpatialComparer ??= new SortingMatch.SpatialAscendingMatchComparer(_indexSearcher, orderByFieldMetadata)
                        : _descSpatialComparer ??= new SortingMatch.SpatialDescendingMatchComparer(_indexSearcher, orderByFieldMetadata);

                    var distance = SpatialUtils.GetGeoDistance(in coordinates, in comparer);
                    documentWithOrderByFields.AddDoubleOrderByField(distance);
                    break;
                }
                default:
                {
                    reader.Read(out var sv);
                    var stringValue = Encoding.UTF8.GetString(sv);
                    documentWithOrderByFields.AddStringOrderByField(stringValue);
                    break;
                }
            }

            d = documentWithOrderByFields;
        }
    }
}

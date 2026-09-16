using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SpatialMatch;
using Spatial4n.Shapes;
using SpatialContext = Spatial4n.Context.SpatialContext;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public IQueryMatch SpatialQuery(in FieldMetadata field, double error, IShape shape, SpatialContext spatialContext, Utils.Spatial.SpatialRelation spatialRelation, in CancellationToken token = default)
    {
        var terms = GetTermsFor(field.FieldName);
        if (terms == null)// If either the term or the field does not exist the request will be empty.
            return EmptyQueryMatch.Instance;

        return field.HasBoost
            ? new SpatialMatch<HasBoosting>(this, _transaction.Allocator, spatialContext, field, shape, terms, error, spatialRelation, token)
            : new SpatialMatch<NoBoosting>(this, _transaction.Allocator, spatialContext, field, shape, terms, error, spatialRelation, token);
    }
}

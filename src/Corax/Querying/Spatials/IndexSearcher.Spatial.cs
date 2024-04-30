using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SpatialMatch;
using Spatial4n.Shapes;
using SpatialContext = Spatial4n.Context.SpatialContext;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public IQueryMatch SpatialQuery(in FieldMetadata field, double error, IShape shape, SpatialContext spatialContext, Utils.Spatial.SpatialRelation spatialRelation, bool isNegated = false, in CancellationToken token = default)
    {
        if (_fieldsTree == null || _fieldsTree.TryGetCompactTreeFor(field.FieldName, out var terms) == false)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(this, Allocator);
        }
        
        IQueryMatch match = field.HasBoost 
            ? new SpatialMatch<HasBoosting>(this, _transaction.Allocator, spatialContext, field, shape, terms, error, spatialRelation, token)
            : new SpatialMatch<NoBoosting>(this, _transaction.Allocator, spatialContext, field, shape, terms, error, spatialRelation, token);
        
        if (isNegated)
        {
            return AndNot(AllEntries(), match);
        }
        
        return match;
    }
}

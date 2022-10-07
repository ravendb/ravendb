using System;
using Corax.Queries;
using Corax.Utils;
using Spatial4n.Shapes;
using SpatialContext = Spatial4n.Context.SpatialContext;

namespace Corax;

public partial class IndexSearcher
{
    public IQueryMatch SpatialQuery(string fieldName, int fieldId, double error, IShape shape, SpatialContext spatialContext, Utils.Spatial.SpatialRelation spatialRelation, bool isNegated = false)
    {
        var terms = _fieldsTree?.CompactTreeFor(fieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(Allocator);
        }

        var match = new SpatialMatch(this, _transaction.Allocator, spatialContext, fieldName, shape, terms, error, fieldId, spatialRelation);
        if (isNegated)
        {
            return AndNot(AllEntries(), match);
        }
        
        return match;
    }
}

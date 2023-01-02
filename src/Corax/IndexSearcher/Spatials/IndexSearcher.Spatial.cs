using System;
using Corax.Mappings;
using Corax.Queries;
using Corax.Utils;
using Spatial4n.Shapes;
using SpatialContext = Spatial4n.Context.SpatialContext;

namespace Corax;

public partial class IndexSearcher
{
    public IQueryMatch SpatialQuery(FieldMetadata field, double error, IShape shape, SpatialContext spatialContext, Utils.Spatial.SpatialRelation spatialRelation, bool isNegated = false)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(this, Allocator);
        }

        var match = new SpatialMatch(this, _transaction.Allocator, spatialContext, field, shape, terms, error, spatialRelation);
        if (isNegated)
        {
            return AndNot(AllEntries(), match);
        }
        
        return match;
    }
}

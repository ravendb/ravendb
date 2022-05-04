using System;
using Corax.Queries;
using Spatial4n.Core.Shapes;
using SpatialContext = Spatial4n.Core.Context.SpatialContext;
using SpatialRelation = Corax.Utils.SpatialRelation;

namespace Corax;

public unsafe partial class IndexSearcher
{
    public SpatialMatch SpatialQuery(string fieldName, int fieldId, double error, IShape shape, SpatialRelation spatialRelation, bool isNegated = false)
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(fieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return default;
        }

        if (isNegated)
        {
            //We have to do the trick and do AndNot();;;;;
            throw new NotImplementedException();
        }
        
        return new SpatialMatch(this, _transaction.Allocator, SpatialContext.GEO, fieldName, shape, terms, error, fieldId, spatialRelation);
    }
}

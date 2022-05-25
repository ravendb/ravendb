using System;
using Corax.Queries;
using Corax.Utils;
using Spatial4n.Core.Shapes;
using SpatialContext = Spatial4n.Core.Context.SpatialContext;

namespace Corax;

public partial class IndexSearcher
{
    public IQueryMatch SpatialQuery(string fieldName, int fieldId, double error, IShape shape, SpatialContext spatialContext, Utils.Spatial.SpatialRelation spatialRelation, bool isNegated = false)
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(fieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty();
        }

        var match = new SpatialMatch(this, _transaction.Allocator, spatialContext, fieldName, shape, terms, error, fieldId, spatialRelation);
        if (isNegated)
        {
            return AndNot(AllEntries(), match);
        }
        
        return match;
    }
}

using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Spatial.BBox;
using Lucene.Net.Spatial.Queries;
using Spatial4n.Core.Context;
using Spatial4n.Core.Shapes;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    internal class BBoxStrategyThatSupportsAllShapes : BBoxStrategy
    {
        public BBoxStrategyThatSupportsAllShapes(SpatialContext ctx, string fieldNamePrefix)
            : base(ctx, fieldNamePrefix)
        {
        }

        private static IRectangle GetRectangle(IShape shape)
        {
            return shape as IRectangle ?? shape.BoundingBox;
        }

        public override AbstractField[] CreateIndexableFields(IShape shape)
        {
            return CreateIndexableFields(GetRectangle(shape));
        }

        public override ConstantScoreQuery MakeQuery(SpatialArgs args)
        {
            args.Shape = GetRectangle(args.Shape);
            return base.MakeQuery(args);
        }

        public override Filter MakeFilter(SpatialArgs args)
        {
            args.Shape = GetRectangle(args.Shape);
            return base.MakeFilter(args);
        }
    }
}

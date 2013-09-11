using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Spatial.BBox;
using Lucene.Net.Spatial.Queries;
using Spatial4n.Core.Context;
using Spatial4n.Core.Shapes;

namespace Raven.Database.Indexing.Spatial
{
	public class BBoxStrategyThatSupportsAllShapes : BBoxStrategy
	{
		public BBoxStrategyThatSupportsAllShapes(SpatialContext ctx, string fieldNamePrefix) : base(ctx, fieldNamePrefix)
		{
		}

		private Rectangle GetRectangle(Shape shape)
		{
			return shape as Rectangle ?? shape.GetBoundingBox();
		}

		public override AbstractField[] CreateIndexableFields(Shape shape)
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
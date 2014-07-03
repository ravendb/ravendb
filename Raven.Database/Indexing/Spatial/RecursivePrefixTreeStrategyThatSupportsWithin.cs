using Lucene.Net.Search;
using Lucene.Net.Spatial.Prefix;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Queries;
using Spatial4n.Core.Shapes;

namespace Raven.Database.Indexing.Spatial
{
	public class RecursivePrefixTreeStrategyThatSupportsWithin : PrefixTreeStrategy
	{
		private int prefixGridScanLevel;

		public RecursivePrefixTreeStrategyThatSupportsWithin(SpatialPrefixTree grid, string fieldName)
			: base(grid, fieldName)
		{
			prefixGridScanLevel = grid.GetMaxLevels() - 4;//TODO this default constant is dependent on the prefix grid size
		}

		public void SetPrefixGridScanLevel(int prefixGridScanLevel)
		{
			//TODO if negative then subtract from maxlevels
			this.prefixGridScanLevel = prefixGridScanLevel;
		}

		public override Filter MakeFilter(SpatialArgs args)
		{
			var op = args.Operation;
			if (!SpatialOperation.Is(op, SpatialOperation.IsWithin, SpatialOperation.Intersects, SpatialOperation.BBoxWithin, SpatialOperation.BBoxIntersects))
				throw new UnsupportedSpatialOperation(op);

			Shape shape = args.Shape;

			int detailLevel = grid.GetLevelForDistance(args.ResolveDistErr(ctx, distErrPct));

			return new RecursivePrefixTreeFilter(GetFieldName(), grid, shape, prefixGridScanLevel, detailLevel);
		}

		public override string ToString()
		{
			return GetType().Name + "(prefixGridScanLevel:" + prefixGridScanLevel + ",SPG:(" + grid + "))";
		}
	}
}
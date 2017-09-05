using Lucene.Net.Search;
using Lucene.Net.Spatial.Prefix;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Queries;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    internal class RecursivePrefixTreeStrategyThatSupportsWithin : PrefixTreeStrategy
    {
        private int _prefixGridScanLevel;

        public RecursivePrefixTreeStrategyThatSupportsWithin(SpatialPrefixTree grid, string fieldName)
            : base(grid, fieldName)
        {
            _prefixGridScanLevel = grid.GetMaxLevels() - 4;//TODO this default constant is dependent on the prefix grid size
        }

        public void SetPrefixGridScanLevel(int prefixGridScanLevel)
        {
            //TODO if negative then subtract from maxlevels
            _prefixGridScanLevel = prefixGridScanLevel;
        }

        public override Filter MakeFilter(SpatialArgs args)
        {
            var op = args.Operation;
            if (SpatialOperation.Is(op, SpatialOperation.IsWithin, SpatialOperation.Intersects, SpatialOperation.BBoxWithin, SpatialOperation.BBoxIntersects) == false)
                throw new UnsupportedSpatialOperation(op);

            var shape = args.Shape;

            var detailLevel = grid.GetLevelForDistance(args.ResolveDistErr(ctx, distErrPct));

            return new RecursivePrefixTreeFilter(GetFieldName(), grid, shape, _prefixGridScanLevel, detailLevel);
        }

        public override string ToString()
        {
            return GetType().Name + "(prefixGridScanLevel:" + _prefixGridScanLevel + ",SPG:(" + grid + "))";
        }
    }
}

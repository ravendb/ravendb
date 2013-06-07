using System.Globalization;
using Raven.Abstractions.Indexing;

namespace Raven.Client.Spatial
{
	public class SpatialCriteriaFactory
	{
		public SpatialCriteria RelatesToShape(object shape, SpatialRelation relation)
		{
			return new SpatialCriteria
				   {
					   Relation = relation,
					   Shape = shape
				   };
		}

		public SpatialCriteria Intersects(object shape)
		{
			return RelatesToShape(shape, SpatialRelation.Intersects);
		}

		public SpatialCriteria Contains(object shape)
		{
			return RelatesToShape(shape, SpatialRelation.Contains);
		}

		public SpatialCriteria Disjoint(object shape)
		{
			return RelatesToShape(shape, SpatialRelation.Disjoint);
		}

		public SpatialCriteria Within(object shape)
		{
			return RelatesToShape(shape, SpatialRelation.Within);
		}

		public SpatialCriteria WithinRadiusOf(double radius, double x, double y)
		{
			var circle = "Circle(" +
						   x.ToString("F6", CultureInfo.InvariantCulture) + " " +
						   y.ToString("F6", CultureInfo.InvariantCulture) + " " +
						   "d=" + radius.ToString("F6", CultureInfo.InvariantCulture) +
						   ")";

			return RelatesToShape(circle, SpatialRelation.Within);

		}
	}
}

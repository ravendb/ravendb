using System.Globalization;
using Raven.Abstractions.Indexing;

namespace Raven.Client
{
	public class SpatialCriteriaFactory
	{
		public SpatialCriteria RelatesToShape(string shape, SpatialRelation relation)
		{
			return new SpatialCriteria
			       {
				       Relation = relation,
					   Shape = shape
			       };
		}

		public SpatialCriteria Within(string shape)
		{
			return RelatesToShape(shape, SpatialRelation.Within);
		}

		public SpatialCriteria Intersects(string shape)
		{
			return RelatesToShape(shape, SpatialRelation.Intersects);
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

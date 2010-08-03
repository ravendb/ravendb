using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Lucene.Net.Util;

using Lucene.Net.Spatial.GeoHash;
using Lucene.Net.Spatial.Geometry;
using Lucene.Net.Spatial.Tier;
using Lucene.Net.Spatial.Tier.Projectors;


namespace Raven.Database.Indexing
{
	public class SpatialIndex
	{
		private static readonly List<CartesianTierPlotter> _ctps = new List<CartesianTierPlotter>();
		private static readonly IProjector _projector = new SinusoidalProjector();

		private const int MinTier = 2;
		private const int MaxTier = 15;

		public const string LatField = "_lat";
		public const string LngField = "_lng";

		static SpatialIndex()
		{
			for (int tier = MinTier; tier <= MaxTier; ++tier)
			{
				_ctps.Add(new CartesianTierPlotter(tier, _projector, CartesianTierPlotter.DefaltFieldPrefix));
			}
		}

		public static string Lat(double value)
		{
			return NumericUtils.DoubleToPrefixCoded(value);
		}

		public static string Lng(double value)
		{
			return NumericUtils.DoubleToPrefixCoded(value);
		}

		public static string Tier(int id, double lat, double lng)
		{
			if (id < MinTier || id > MaxTier)
			{
				throw new ArgumentException(
					string.Format("id should be between {0} and {1}", MinTier, MaxTier), "id");
			}

			var boxId = _ctps[id - MinTier].GetTierBoxId(lat, lng);

			return NumericUtils.DoubleToPrefixCoded(boxId);
		}
	}
}

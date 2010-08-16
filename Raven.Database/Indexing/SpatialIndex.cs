using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

using Lucene.Net.Util;
using Lucene.Net.Spatial.Tier;
using Lucene.Net.Spatial.Tier.Projectors;


namespace Raven.Database.Indexing
{
	public static class SpatialIndex
	{
		private static readonly List<CartesianTierPlotter> Ctps = new List<CartesianTierPlotter>();
		private static readonly IProjector Projector = new SinusoidalProjector();

		private static readonly Regex RegexSelectNew = new Regex(@"select\s+new\s*\{([^\}]+)\}", RegexOptions.IgnoreCase);

		private const int MinTier = 2;
		private const int MaxTier = 15;

		public const string LatField = "latitude";
		public const string LngField = "longitude";

		static SpatialIndex()
		{
			for (int tier = MinTier; tier <= MaxTier; ++tier)
			{
				Ctps.Add(new CartesianTierPlotter(tier, Projector, CartesianTierPlotter.DefaltFieldPrefix));
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
					string.Format("tier id should be between {0} and {1}", MinTier, MaxTier), "id");
			}

			var boxId = Ctps[id - MinTier].GetTierBoxId(lat, lng);

			return NumericUtils.DoubleToPrefixCoded(boxId);
		}

		public static double GetDistanceMi(double x1, double y1, double x2, double y2)
		{
			return DistanceUtils.GetInstance().GetDistanceMi(x1, y1, x2, y2);
		}

		public static IndexDefinition ToSpatial(this IndexDefinition definition, string latAccessor, string lngAccessor)
		{
			if (!string.IsNullOrEmpty(definition.Reduce))
			{
				throw new ArgumentException("IndexDefinition.ToSpatial() not supported for map reduce indexes");
			}

			if (definition.Map.Contains("=>"))
			{
				throw new ArgumentException("IndexDefinition.ToSpatial() not supported for linq method indexes");
			}

			if (!RegexSelectNew.IsMatch(definition.Map))
			{
				throw new ArgumentException("IndexDefinition.ToSpatial() supported only for indexes like select new { ... }");
			}

			var fields = new StringBuilder();

			fields.AppendFormat(", latitude = SpatialIndex.Lat({0})", latAccessor);
			fields.AppendFormat(", longitude = SpatialIndex.Lng({0})", lngAccessor);

			for (int id = MinTier; id <= MaxTier; ++id)
			{
				fields.AppendFormat(", _tier_{0} = SpatialIndex.Tier({0}, {1}, {2})", id, latAccessor, lngAccessor);
			}

			definition.Map = RegexSelectNew.Replace(definition.Map, "select new { $1 " + fields + " }");

			definition.Stores["latitude"] = FieldStorage.Yes;
			definition.Stores["longitude"] = FieldStorage.Yes;

			for (int id = MinTier; id <= MaxTier; ++id)
			{
				definition.Stores["_tier_" + id] = FieldStorage.Yes;
			}

			definition.Indexes["latitude"] = FieldIndexing.NotAnalyzed;
			definition.Indexes["longitude"] = FieldIndexing.NotAnalyzed;

			for (int id = MinTier; id <= MaxTier; ++id)
			{
				definition.Indexes["_tier_" + id] = FieldIndexing.NotAnalyzedNoNorms;
			}

			return definition;
		}
	}
}

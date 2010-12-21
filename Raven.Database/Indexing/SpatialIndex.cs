//-----------------------------------------------------------------------
// <copyright file="SpatialIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Documents;
using Lucene.Net.Util;
using Lucene.Net.Spatial.Tier;
using Lucene.Net.Spatial.Tier.Projectors;


namespace Raven.Database.Indexing
{
	public static class SpatialIndex
	{
		private static readonly List<CartesianTierPlotter> Ctps = new List<CartesianTierPlotter>();
		private static readonly IProjector Projector = new SinusoidalProjector();

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

		public static IEnumerable<AbstractField> Generate(double lat, double lng)
		{
			yield return new Field("latitude", Lat(lat),Field.Store.YES, Field.Index.NOT_ANALYZED);
			yield return new Field("longitude", Lng(lng), Field.Store.YES, Field.Index.NOT_ANALYZED);

			for (var id = MinTier; id <= MaxTier; ++id)
			{
				yield return new Field("_tier_" + id, Tier(id, lat, lng),Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
			}
		}
	}
}

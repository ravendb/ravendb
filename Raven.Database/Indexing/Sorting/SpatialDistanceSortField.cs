using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Spatial4n.Core.Exceptions;
using Spatial4n.Core.Shapes;

namespace Raven.Database.Indexing.Sorting
{
	public class SpatialDistanceSortField : SortField
	{
		private readonly double lng, lat;

		public SpatialDistanceSortField(string field, bool reverse, SpatialIndexQuery qry) : base(field, CUSTOM, reverse)
		{
			lat = qry.Latitude;
			lng = qry.Longitude;
		}

		public override FieldComparator GetComparator(int numHits, int sortPos)
		{
			return new SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator(lat, lng, numHits);
		}

		public override FieldComparatorSource GetComparatorSource()
		{
			return new SpatialDistanceFieldComparatorSource(lat, lng);
		}
	}

	public class SpatialDistanceFieldComparatorSource : FieldComparatorSource
	{
		protected readonly double lng, lat;

		public SpatialDistanceFieldComparatorSource(double lat, double lng)
		{
			this.lat = lat;
			this.lng = lng;
		}

		public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
		{
			return new SpatialDistanceFieldComparator(lat, lng, numHits);
		}

		public class SpatialDistanceFieldComparator : FieldComparator
		{
			private readonly double[] values;
			private double bottom;
			private readonly Point originPt;

			private int currentDocBase;
			private IndexReader currentIndexReader;

			public SpatialDistanceFieldComparator(double lat, double lng, int numHits)
			{
				values = new double[numHits];
				originPt = SpatialIndex.Context.MakePoint(lng, lat);
			}

			public override int Compare(int slot1, int slot2)
			{
				double a = values[slot1];
				double b = values[slot2];
				if (a > b)
					return 1;
				if (a < b)
					return -1;

				return 0;
			}

			public override void SetBottom(int slot)
			{
				bottom = values[slot];
			}

			public override int CompareBottom(int doc)
			{
				var v2 = CalculateDistance(doc);
				if (bottom > v2)
				{
					return 1;
				}

				if (bottom < v2)
				{
					return -1;
				}

				return 0;
			}

			public override void Copy(int slot, int doc)
			{
				values[slot] = CalculateDistance(doc);
			}

			private double CalculateDistance(int doc)
			{
				var document = currentIndexReader.Document(doc);
				if (document == null)
					return double.NaN;
				var field = document.GetField(Constants.SpatialShapeFieldName);
				if(field == null)
					return double.NaN;
				var shapeAsText = field.StringValue();
				Shape shape;
				try
				{
					shape = SpatialIndex.Context.ReadShape(shapeAsText);
				}
				catch (InvalidOperationException)
				{
					return double.NaN;
				}
				var pt = shape as Point;
				return SpatialIndex.Context.GetDistCalc().Distance(pt, originPt);
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				currentIndexReader = reader;
				currentDocBase = docBase;
			}

			public override IComparable Value(int slot)
			{
				return values[slot];
			}

		}
	}
}

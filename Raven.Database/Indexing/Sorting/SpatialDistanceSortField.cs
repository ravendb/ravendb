using System;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Spatial4n.Core.Shapes;

namespace Raven.Database.Indexing.Sorting
{
	public class SpatialDistanceSortField : SortField
	{
		private readonly Point center;

		public SpatialDistanceSortField(string field, bool reverse, SpatialIndexQuery qry)
			: base(field, CUSTOM, reverse)
		{
			var shape = SpatialIndex.Context.ReadShape(qry.QueryShape);
			center = shape.GetCenter();
		}

		public override FieldComparator GetComparator(int numHits, int sortPos)
		{
			return new SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator(center, numHits);
		}

		public override FieldComparatorSource ComparatorSource
		{
			get
			{
				return new SpatialDistanceFieldComparatorSource(center);
			}
		} 
	}

	public class SpatialDistanceFieldComparatorSource : FieldComparatorSource
	{
		private readonly Point center;

		public SpatialDistanceFieldComparatorSource(Point center)
		{
			this.center = center;
		}

		public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
		{
			return new SpatialDistanceFieldComparator(center, numHits);
		}

		public class SpatialDistanceFieldComparator : FieldComparator
		{
			private readonly double[] values;
			private double bottom;
			private readonly Point originPt;

			private IndexReader currentIndexReader;

			public SpatialDistanceFieldComparator(Point origin, int numHits)
			{
				values = new double[numHits];
				originPt = origin;
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
				var shapeAsText = field.StringValue;
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
			}

			public override IComparable this[int slot]
			{
				get { return values[slot]; }
			}
		}
	}
}

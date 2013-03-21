using System;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Spatial4n.Core.Shapes;

namespace Raven.Database.Indexing.Sorting
{
	public struct DistanceValue : IComparable
	{
		public double Value;
		public int CompareTo(object obj)
		{
			if (obj == null)
				return 1;
			return Value.CompareTo(((DistanceValue) obj).Value);
		}
	}

	public class SpatialDistanceFieldComparatorSource : FieldComparatorSource
	{
		private readonly Point center;
	    private readonly SpatialField spatialField;

		public SpatialDistanceFieldComparatorSource(SpatialField spatialField, Point center)
		{
		    this.spatialField = spatialField;
			this.center = center;
		}

		public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
		{
			return new SpatialDistanceFieldComparator(spatialField, center, numHits);
		}

		public class SpatialDistanceFieldComparator : FieldComparator
		{
			private readonly SpatialField spatialField;
			private readonly DistanceValue[] values;
			private DistanceValue bottom;
			private readonly Point originPt;

			private IndexReader currentIndexReader;

			public SpatialDistanceFieldComparator(SpatialField spatialField, Point origin, int numHits)
			{
				this.spatialField = spatialField;
				values = new DistanceValue[numHits];
				originPt = origin;
			}

			public override int Compare(int slot1, int slot2)
			{
				var a = values[slot1];
				var b = values[slot2];
				if (a.Value > b.Value)
					return 1;
				if (a.Value < b.Value)
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
				if (bottom.Value > v2)
				{
					return 1;
				}

				if (bottom.Value < v2)
				{
					return -1;
				}

				return 0;
			}

			public override void Copy(int slot, int doc)
			{
				values[slot] = new DistanceValue
				{
					Value = CalculateDistance(doc)
				};
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
					shape = spatialField.ReadShape(shapeAsText);
				}
				catch (InvalidOperationException)
				{
					return double.NaN;
				}
				var pt = shape as Point;
				if (pt == null)
					pt = shape.GetCenter();
				return spatialField.GetContext().GetDistCalc().Distance(pt, originPt);
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

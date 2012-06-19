using System;
using System.Diagnostics;
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
			private double[] currentReaderValues;
			private double bottom;
			private readonly Point originPt;

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
				var v2 = currentReaderValues[doc];
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
				values[slot] = currentReaderValues[doc];
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				currentReaderValues = ComputeDistances(reader);
			}

			public override IComparable Value(int slot)
			{
				return values[slot];
			}

			protected internal double[] ComputeDistances(IndexReader reader)
			{
				double[] retArray = null;
				var termDocs = reader.TermDocs();
				var termEnum = reader.Terms(new Term(Constants.SpatialShapeFieldName));
				try
				{
					do
					{
						Term term = termEnum.Term();
						if (term == null)
							break;

						Debug.Assert(Constants.SpatialShapeFieldName.Equals(term.Field()));

						Shape termval;
						try
						{
							termval = SpatialIndex.Context.ReadShape(term.Text()); // read shape
						}
						catch (InvalidShapeException)
						{
							continue;
						}

						var pt = termval as Point;
						if (pt == null)
							continue;

						var distance = SpatialIndex.Context.GetDistCalc().Distance(pt, originPt);

						if (retArray == null)
							// late init
							retArray = new double[reader.MaxDoc()];
						termDocs.Seek(termEnum);
						while (termDocs.Next())
						{
							retArray[termDocs.Doc()] = distance;
						}
					} while (termEnum.Next());
				}
				finally
				{
					termDocs.Close();
					termEnum.Close();
				}
				return retArray ?? new double[reader.MaxDoc()];
			}
		}
	}
}

using System;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Spatial4n.Core.Shapes;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    public class SpatialDistanceFieldComparatorSource : FieldComparatorSource
    {
        private readonly Point _center;
        private readonly SpatialField _spatialField;

        public SpatialDistanceFieldComparatorSource(SpatialField spatialField, Point center)
        {
            _spatialField = spatialField;
            _center = center;
        }

        public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
        {
            return new SpatialDistanceFieldComparator(_spatialField, _center, numHits);
        }

        public class SpatialDistanceFieldComparator : FieldComparator
        {
            private readonly SpatialField _spatialField;
            private readonly DistanceValue[] _values;
            private DistanceValue _bottom;
            private readonly Point _originPt;

            private IndexReader _currentIndexReader;

            public SpatialDistanceFieldComparator(SpatialField spatialField, Point origin, int numHits)
            {
                _spatialField = spatialField;
                _values = new DistanceValue[numHits];
                _originPt = origin;
            }

            public override int Compare(int slot1, int slot2)
            {
                var a = _values[slot1];
                var b = _values[slot2];
                if (a.Value > b.Value)
                    return 1;
                if (a.Value < b.Value)
                    return -1;

                return 0;
            }

            public override void SetBottom(int slot)
            {
                _bottom = _values[slot];
            }

            public override int CompareBottom(int doc, IState state)
            {
                var v2 = CalculateDistance(doc, state);
                if (_bottom.Value > v2)
                {
                    return 1;
                }

                if (_bottom.Value < v2)
                {
                    return -1;
                }

                return 0;
            }

            public override void Copy(int slot, int doc, IState state)
            {
                _values[slot] = new DistanceValue
                {
                    Value = CalculateDistance(doc, state)
                };
            }

            private double CalculateDistance(int doc, IState state)
            {
                var document = _currentIndexReader.Document(doc, state);
                if (document == null)
                    return double.NaN;
                var field = document.GetField(Constants.Documents.Indexing.Fields.SpatialShapeFieldName);
                if (field == null)
                    return double.NaN;
                var shapeAsText = field.StringValue(state);
                Shape shape;
                try
                {
                    shape = _spatialField.ReadShape(shapeAsText);
                }
                catch (InvalidOperationException)
                {
                    return double.NaN;
                }
                var pt = shape as Point;
                if (pt == null)
                    pt = shape.GetCenter();
                return _spatialField.GetContext().GetDistCalc().Distance(pt, _originPt);
            }

            public override void SetNextReader(IndexReader reader, int docBase, IState state)
            {
                _currentIndexReader = reader;
            }

            public override IComparable this[int slot] => _values[slot];
        }

        private struct DistanceValue : IComparable
        {
            public double Value;
            public int CompareTo(object obj)
            {
                if (obj == null)
                    return 1;
                return Value.CompareTo(((DistanceValue)obj).Value);
            }
        }
    }
}

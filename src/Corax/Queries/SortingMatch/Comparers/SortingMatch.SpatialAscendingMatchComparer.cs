using System;
using System.IO;
using System.Runtime.CompilerServices;
using Corax.Utils;
using Corax.Utils.Spatial;
using Spatial4n.Shapes;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    public struct SpatialAscendingMatchComparer : ISpatialComparer
    {
        private readonly IndexSearcher _searcher;
        private readonly int _fieldId;
        private readonly delegate*<ref SpatialAscendingMatchComparer, long, long, int> _compareFunc;
        private readonly MatchCompareFieldType _fieldType;
        private readonly IPoint _point;
        private readonly double _round;
        private readonly SpatialUnits _units;
        
        public IPoint Point => _point;

        public double Round => _round;

        public SpatialUnits Units => _units;
        public int FieldId => _fieldId;
        public MatchCompareFieldType FieldType => _fieldType;

        public SpatialAscendingMatchComparer(IndexSearcher searcher, in OrderMetadata metadata)
        {
            _searcher = searcher;
            _fieldId = metadata.FieldId;
            _fieldType = metadata.FieldType;
            _point = metadata.Point;
            _round = metadata.Round;
            _units = metadata.Units;
            

            static int ThrowOnWrongEntryFieldType(ref SpatialAscendingMatchComparer comparer, long x, long y)
            {
                throw new InvalidDataException($"{nameof(SpatialAscendingMatchComparer)} is only for spatial data.");
            }
            
            static int CompareWithSpatialLoad<T>(ref SpatialAscendingMatchComparer comparer, long x, long y) where T : unmanaged
            {
                var readerX = comparer._searcher.GetReaderFor(x);
                var readX = readerX.Read(comparer._fieldId, out (double lat, double lon) resultX);

                var readerY = comparer._searcher.GetReaderFor(y);
                var readY = readerY.Read(comparer._fieldId, out (double lat, double lon) resultY);

                if (readX && readY)
                {
                    var readerXDistance = SpatialUtils.GetGeoDistance(in resultX, comparer);
                    var readerYDistance = SpatialUtils.GetGeoDistance(in resultY, comparer);
                    
                    return comparer.CompareNumerical(readerXDistance, readerYDistance);
                }
                else if (readX)
                    return 1;

                return -1;
            }
            
            _compareFunc = _fieldType switch
            {
                MatchCompareFieldType.Sequence => &ThrowOnWrongEntryFieldType,
                MatchCompareFieldType.Integer => &ThrowOnWrongEntryFieldType,
                MatchCompareFieldType.Floating => &ThrowOnWrongEntryFieldType,
                MatchCompareFieldType.Spatial => &CompareWithSpatialLoad<double>,
                var type => throw new NotSupportedException($"Currently, we do not support sorting by {type}.")
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareById(long idx, long idy)
        {
            return _compareFunc(ref this, idx, idy);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareNumerical<T>(T sx, T sy) where T : unmanaged
        {
            return BasicComparers.CompareAscending(sx, sy);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return BasicComparers.CompareAscending(sx, sy);
        }
    }
}

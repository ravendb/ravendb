using System;
using System.Collections.Generic;
using System.Globalization;
using Corax.Utils;
using Sparrow.Server;
using Spatial4n.Core.Context;
using Spatial4n.Core.Shapes;
using Spatial4n.Core.Shapes.Impl;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries;

public class SpatialMatch : IQueryMatch
{
    private readonly IndexSearcher _indexSearcher;
    private readonly SpatialContext _spatialContext;
    private readonly ExtendedEntryFieldType _extendedEntryFieldType;
    private readonly string _fieldName;
    private readonly double _error;
    private readonly IShape _shape;
    private readonly CompactTree _tree;
    private IEnumerator<(string Geohash, bool isTermMatch)> _termGenerator;
    private TermMatch _currentMatch;
    private readonly ByteStringContext _allocator;
    private readonly int _fieldId;
    private readonly SpatialHelper.SpatialRelation _spatialRelation;
    private bool _isTermMatch;
    private IDisposable _startsWithDisposeHandler;
    private Slice _startsWith;
    private HashSet<long> _alreadyReturned;

    public SpatialMatch(IndexSearcher indexSearcher, ByteStringContext allocator, SpatialContext spatialContext, string fieldName, IShape shape,
        CompactTree tree,
        double errorInPercentage, int fieldId, SpatialHelper.SpatialRelation spatialRelation)
    {
        _fieldId = fieldId;
        _indexSearcher = indexSearcher;
        _spatialContext = spatialContext ?? throw new NullReferenceException("SpatialContext is null");
        _fieldName = fieldName;
        _error = SpatialHelper.GetErrorFromPercentage(spatialContext, shape, errorInPercentage);
        _shape = shape;
        _tree = tree;
        _allocator = allocator;
        _spatialRelation = spatialRelation;
        _termGenerator = spatialRelation == SpatialHelper.SpatialRelation.Disjoint 
            ? SpatialHelper.GetGeohashesForQueriesOutsideShape(_indexSearcher, tree, allocator, spatialContext, shape).GetEnumerator() 
            : SpatialHelper.GetGeohashesForQueriesInsideShape(_indexSearcher, tree, allocator, spatialContext, shape).GetEnumerator();
        GoNextMatch();
    }

    private bool GoNextMatch()
    {
        if (_termGenerator.MoveNext())
        {
            var result = _termGenerator.Current;
            _startsWithDisposeHandler?.Dispose();
            _startsWithDisposeHandler = Slice.From(_allocator, result.Geohash, out var term);
            _isTermMatch = result.isTermMatch;
            _currentMatch = _indexSearcher.TermQuery(_tree, term);

            return true;
        }
        _currentMatch = TermMatch.CreateEmpty();
        return false;
    }

    public long Count => long.MaxValue;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting { get; }

    public int Fill(Span<long> matches)
    {
        int currentIdx = 0;
        do
        {
            int read;
            if ((read = _currentMatch.Fill(matches.Slice(currentIdx))) == 0)
            {
                if (GoNextMatch() == false)
                {
                    return currentIdx;
                }

                continue;
            }

            if (_isTermMatch)
            {
                currentIdx += read;
            }
            else if (read > 0)
            {
                for (int i = 0; i < read; ++i)
                {
                    if (CheckEntryManually(matches[i]))
                    {
                        matches[currentIdx++] = matches[i];
                    }
                }
            }
        } while (currentIdx != matches.Length);

        return currentIdx;
    }

    private bool CheckEntryManually(long id)
    {
        var reader = _indexSearcher.GetReaderFor(id);
        var type = reader.GetFieldType(_fieldId, out _);
        if (_alreadyReturned?.TryGetValue(id, out _) ?? false)
        {
            return false;
        }
        
        if (type.HasFlag(IndexEntryFieldType.List))
        {
            _alreadyReturned ??= new HashSet<long>();
            var iterator = reader.ReadManySpatialPoint(_fieldId);
            while (iterator.ReadNext())
            {
                var point = new Point(iterator.Longitude, iterator.Latitude, _spatialContext);
                if (IsTrue(point.Relate(_shape)))
                {
                    _alreadyReturned.Add(id);
                    return true;
                }
            }
        }
        else
        {
            reader.Read(_fieldId, out (double Lat, double Lon) coorinate);
            var point = new Point(coorinate.Lon, coorinate.Lat, _spatialContext);
            if (IsTrue(point.Relate(_shape)))
            {
                return true;
            }
        }

        return false;
    }
    
    public bool IsTrue(SpatialRelation answer) => answer switch
    {
        SpatialRelation.WITHIN or SpatialRelation.CONTAINS => _spatialRelation is SpatialHelper.SpatialRelation.Within
            or SpatialHelper.SpatialRelation.Contains,
        SpatialRelation.DISJOINT => _spatialRelation is SpatialHelper.SpatialRelation.Disjoint,
        SpatialRelation.INTERSECTS => _spatialRelation is SpatialHelper.SpatialRelation.Intersects,
        _ => throw new NotSupportedException()
    };

    public int AndWith(Span<long> buffer, int matches)
    {
        var currentIdx = 0;
        for (int i = 0; i < matches; ++i)
        {
            var reader = _indexSearcher.GetReaderFor(buffer[i]);
            reader.Read(_fieldId, out (double Lat, double Lon) coorinate);
            var point = new Point(coorinate.Lon, coorinate.Lat, _spatialContext);
            if (IsTrue(point.Relate(_shape)))
            {
                buffer[currentIdx++] = buffer[i];
            }
        }

        return currentIdx;
    }

    public void Score(Span<long> matches, Span<float> scores)
    {
        throw new NotImplementedException();
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(StartWithTermProvider)}",
            parameters: new Dictionary<string, string>()
            {
                {"Field", _fieldName},
                {"FieldId", _fieldId.ToString()},
                {"Shape", _shape.ToString()},
                {"Error", _error.ToString(CultureInfo.InvariantCulture)},
                {"SpatialRelation", _spatialRelation.ToString()},
            });
    }
}

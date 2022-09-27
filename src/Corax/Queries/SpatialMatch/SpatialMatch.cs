using System;
using System.Collections.Generic;
using System.Globalization;
using Corax.Utils.Spatial;
using Sparrow.Server;
using Spatial4n.Context;
using Spatial4n.Shapes;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Debugging;
using SpatialRelation = Spatial4n.Shapes.SpatialRelation;

namespace Corax.Queries;

public class SpatialMatch : IQueryMatch
{
    private readonly IndexSearcher _indexSearcher;
    private readonly SpatialContext _spatialContext;
    private readonly string _fieldName;
    private readonly double _error;
    private readonly IShape _shape;
    private readonly CompactTree _tree;
    private IEnumerator<(string Geohash, bool isTermMatch)> _termGenerator;
    private TermMatch _currentMatch;
    private readonly ByteStringContext _allocator;
    private readonly int _fieldId;
    private readonly Utils.Spatial.SpatialRelation _spatialRelation;
    private bool _isTermMatch;
    private IDisposable _startsWithDisposeHandler;
    private HashSet<long> _alreadyReturned;
    private Slice _fieldNameSlice;

    public SpatialMatch(IndexSearcher indexSearcher, ByteStringContext allocator, SpatialContext spatialContext, string fieldName, IShape shape,
        CompactTree tree,
        double errorInPercentage, int fieldId, Utils.Spatial.SpatialRelation spatialRelation)
    {
        _fieldId = fieldId;
        _indexSearcher = indexSearcher;
        _spatialContext = spatialContext ?? throw new ArgumentNullException($"{nameof(spatialContext)} passed to {nameof(SpatialMatch)} is null.");
        _fieldName = fieldName;
        Slice.From(allocator, fieldName, out _fieldNameSlice);
        _error = SpatialUtils.GetErrorFromPercentage(spatialContext, shape, errorInPercentage);
        _shape = shape;
        _tree = tree;
        _allocator = allocator;
        _spatialRelation = spatialRelation;
        _termGenerator = spatialRelation == Utils.Spatial.SpatialRelation.Disjoint 
            ? SpatialUtils.GetGeohashesForQueriesOutsideShape(_indexSearcher, tree, allocator, spatialContext, shape).GetEnumerator() 
            : SpatialUtils.GetGeohashesForQueriesInsideShape(_indexSearcher, tree, allocator, spatialContext, shape).GetEnumerator();
        GoNextMatch();
        DebugStuff.RenderAndShow(tree);
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
        _currentMatch = TermMatch.CreateEmpty(_indexSearcher.Allocator);
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
                    break;
                }

                continue;
            }

            if (_isTermMatch)
            {
                currentIdx += read;
            }
            else if (read > 0)
            {
                var slicedMatches = matches.Slice(currentIdx);
                for (int i = 0; i < read; ++i)
                {
                    if (CheckEntryManually(slicedMatches[i]))
                    {
                        matches[currentIdx++] = slicedMatches[i];
                    }
                }
            }
        } while (currentIdx != matches.Length);

        return currentIdx;
    }

    private bool CheckEntryManually(long id)
    {
        var reader = _indexSearcher.GetReaderFor(id);
        var fieldReader = reader.GetReaderFor(_fieldNameSlice, _fieldId);
        if (_alreadyReturned?.TryGetValue(id, out _) ?? false)
        {
            return false;
        }

        if (fieldReader.TryReadManySpatialPoint(out var iterator))
        {
            _alreadyReturned ??= new HashSet<long>();
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
        else if (fieldReader.Read(out (double Lat, double Lon) coorinate))
        {
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
        SpatialRelation.Within or SpatialRelation.Contains => _spatialRelation is Utils.Spatial.SpatialRelation.Within
            or Utils.Spatial.SpatialRelation.Contains,
        SpatialRelation.Disjoint => _spatialRelation is Utils.Spatial.SpatialRelation.Disjoint,
        SpatialRelation.Intersects => _spatialRelation is Utils.Spatial.SpatialRelation.Intersects,
        _ => throw new NotSupportedException()
    };

    public int AndWith(Span<long> buffer, int matches)
    {
        var currentIdx = 0;
        for (int i = 0; i < matches; ++i)
        {
            var reader = _indexSearcher.GetReaderFor(buffer[i]);
            var entryReader = reader.GetReaderFor(_fieldNameSlice, _fieldId);
            if (entryReader.Read(out (double Lat, double Lon) coorinate))
            {
                var point = new Point(coorinate.Lon, coorinate.Lat, _spatialContext);
                if (IsTrue(point.Relate(_shape)))
                {
                    buffer[currentIdx++] = buffer[i];
                }
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

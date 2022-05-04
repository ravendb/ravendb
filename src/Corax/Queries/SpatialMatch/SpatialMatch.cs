using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Corax.Utils;
using Sparrow.Server;
using Spatial4n.Core.Context;
using Spatial4n.Core.Shapes;
using Spatial4n.Core.Shapes.Impl;
using Voron;
using Voron.Data.CompactTrees;
using SpatialRelation = Corax.Utils.SpatialRelation;

namespace Corax.Queries;

public class SpatialMatch : IQueryMatch
{
    private readonly IndexSearcher _indexSearcher;
    private readonly SpatialContext _spatialContext;
    private readonly SpecialEntryFieldType _specialEntryFieldType;
    private readonly string _fieldName;
    private readonly double _error;
    private readonly IShape _shape;
    private readonly List<string> _geohashsTermMatch;
    private readonly List<string> _geohashsNeedsToBeChecked;
    private readonly CompactTree _tree;


    private int currentGeohashId = 0;
    private StartWithTermProvider _startsWithProvider;
    private TermMatch _currentMatch;
    private readonly ByteStringContext _allocator;
    private readonly int _fieldId;
    private readonly SpatialRelation _spatialRelation;
    private bool _isTermMatch;
    private int _alreadySeen = 0;
    private IDisposable _startsWithDisposeHandler;
    private Slice _startsWith;
    private Trie _trie;

    public SpatialMatch(IndexSearcher indexSearcher, ByteStringContext allocator, Spatial4n.Core.Context.SpatialContext spatialContext, string fieldName, IShape shape,
        CompactTree tree,
        double errorInPercentage, int fieldId, SpatialRelation spatialRelation)
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
        _trie = new();
        //We build two lists while filling in the shape. The first one is filled with geohashs that
        //are definitely in the body of the figure. This allows us to create a TermMatch. The second list
        //is filled with boundary points, which we are not able to determine (unless we build more and more
        //accurate meshes for the figure, which is very expensive). In this case, we need to check all the
        //hashes with prefix X, and evaluate each of them individually if they are in our figure.
        SpatialHelper.FulfillShape(SpatialContext.GEO, shape, _geohashsTermMatch = new(), _geohashsNeedsToBeChecked = new(), maxPrecision: 4);
        
        _alreadyReturned = new HashSet<Slice>(SliceComparer.Instance);

        // At the beginning we are will returns ALL items that we are sure 
        _isTermMatch = _geohashsTermMatch.Count > 0;

        GoNextMatch();
    }

    private int _geoTermMatchId = 0;
    private int _geoStartsWithId = 0;
    
    /// <summary>
    ///  We've persist all prefixes already returned to the user.
    /// This is only for startsWith calls. This happends because we have to p
    /// </summary>
    private HashSet<Slice> _alreadyReturned;

    private bool GoNextMatch(bool requireReload = false)
    {
        if (_isTermMatch && _geohashsTermMatch.Count > 0 && _geoTermMatchId < _geohashsTermMatch.Count)
        {
            _startsWithDisposeHandler = Slice.From(_allocator, _geohashsTermMatch[_geoTermMatchId], out _startsWith);
            _currentMatch = _indexSearcher.TermQuery(_fieldName, _startsWith);
            
            _geoTermMatchId++;

            return true;
        }

        if (_isTermMatch)
        {
            //We are out of TermMatches. Now we are gonna do SEEK and check every single element;
            _isTermMatch = false;

            if (_geohashsNeedsToBeChecked.Count == 0)
            {
                return false;
            }

            StartsWithFetcher();
        }


        while (true)
        {
            //   _currentMatch.
            if (_startsWithProvider.Next(out _currentMatch, out _startsWith) == false || requireReload)
            {
                if (_geohashsNeedsToBeChecked.Count == 0 || _geohashsNeedsToBeChecked.Count <= _geoStartsWithId)
                {
                    return false;
                }

                StartsWithFetcher();
            }
            else
            {
                _trie.TryGetValue(_startsWith, out var alreadyReturned);
                if (alreadyReturned)
                    continue;
                
                return true;
            }
        }


        void StartsWithFetcher()
        {
            //_startsWithDisposeHandler?.Dispose();
            _startsWithDisposeHandler = Slice.From(_allocator, _geohashsNeedsToBeChecked[_geoStartsWithId], out _startsWith);
            _startsWithProvider = new(_indexSearcher, _allocator, _tree, _fieldName, _fieldId, _startsWith);
            _geoStartsWithId++;
        }
    }

    public long Count => throw new NotSupportedException();
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
            else
            {
                var figure = Spatial4n.Core.Util.GeohashUtils.DecodeBoundary(_startsWith.ToString(), _spatialContext);
                if (IsTrue(figure.Relate(_shape)))
                {
                    _trie.Add(_startsWith, true);
                    GoNextMatch(true);
                    currentIdx += read;
                }
            }
        } while (currentIdx != matches.Length);

        return currentIdx;
    }

    public bool IsTrue(Spatial4n.Core.Shapes.SpatialRelation answer) => answer switch
    {
        Spatial4n.Core.Shapes.SpatialRelation.WITHIN or Spatial4n.Core.Shapes.SpatialRelation.CONTAINS => _spatialRelation is SpatialRelation.Within
            or SpatialRelation.Contains,
        Spatial4n.Core.Shapes.SpatialRelation.DISJOINT => _spatialRelation is SpatialRelation.Disjoint,
        Spatial4n.Core.Shapes.SpatialRelation.INTERSECTS => true,
        _ => throw new NotSupportedException()
    };

    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotImplementedException();
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
                {"TermMatchGeoHashs", string.Join(", ", _geohashsTermMatch)},
                {"GeohashsToCheck", string.Join(", ", _geohashsNeedsToBeChecked)},
                {"Shape", _shape.ToString()},
                {"Error", _error.ToString(CultureInfo.InvariantCulture)},
                {"SpatialRelation", _spatialRelation.ToString()},
            });
    }

    private string GenerateJSListOfGeohashsForGEOJsonVisualiser()
    {
        var json = _geohashsTermMatch.Concat(_geohashsNeedsToBeChecked).ToList();
        var sb = new StringBuilder();
        sb.Append('[');
        bool notAddComa = true;
        foreach (var jItem in json)
        {
            if (notAddComa == false)
                sb.Append(',');
            else
                notAddComa = false;

            sb.Append($"'{jItem}'");
        }

        sb.Append("];");
        return sb.ToString();
    }
}

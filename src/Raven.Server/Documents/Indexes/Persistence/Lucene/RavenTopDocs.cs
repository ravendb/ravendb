using System;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public class RavenTopDocs : TopDocs
{
    private readonly UnmanagedScoreDocArray _unmanagedScoreDocs;

    public RavenTopDocs()
    {
        _unmanagedScoreDocs = new UnmanagedScoreDocArray();
    }

    public override int Count => _unmanagedScoreDocs.Length;

    public override ScoreDoc[] ScoreDocs
    {
        get
        {
            throw new NotSupportedException($"In order to access the ScoreDocs use {nameof(GetRawValues)} instead");
        }
        set
        {
            throw new NotSupportedException($"You cannot set the {nameof(ScoreDocs)}");
        }
    }

    public override ScoreDoc this[int index]
    {
        get
        {
            throw new NotSupportedException($"Use {nameof(GetRawValues)} instead");
        }
    }

    public override (int Doc, float Score) GetRawValues(int index)
    {
        var cds = _unmanagedScoreDocs[index];
        return (cds.Doc, cds.Score);
    }

    public void Add(int doc, float score)
    {
        _unmanagedScoreDocs.Add(doc, score);
    }

    public override void Dispose()
    {
        base.Dispose();
        _unmanagedScoreDocs?.Dispose();
    }
}

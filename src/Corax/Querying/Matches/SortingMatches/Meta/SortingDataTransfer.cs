using Corax.Utils.Spatial;

namespace Corax.Querying.Matches.SortingMatches.Meta;

public struct SortingDataTransfer
{
    public bool IncludeScores => ScoresBuffer is {Length: > 0};
    public bool IncludeDistances => DistancesBuffer is {Length: > 0};
    
    public float[] ScoresBuffer;
    public SpatialResult[] DistancesBuffer;
}

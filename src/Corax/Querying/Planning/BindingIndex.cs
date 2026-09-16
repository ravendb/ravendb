namespace Corax.Querying.Planning;

/// <summary>Named binding indices per clause type. Each clause type stores its
/// parameter bindings in a flat array at these known positions.</summary>
public static class BindingIndex
{
    // Equals, NotEquals, Range (GT/GTE/LT/LTE), StartsWith, EndsWith, Search, Regex:
    public const int Value = 0;

    // Between:
    public const int BetweenLow = 0;
    public const int BetweenHigh = 1;

    // Spatial circle: [0]=distErrPct, [1]=radius, [2]=lat, [3]=lng, [4]=units
    public const int SpatialCircleBindingCount = 5; // distErrPct + radius + lat + lng + units
    public const int SpatialDistErrPct = 0;
    public const int SpatialRadius = 1;
    public const int SpatialLatitude = 2;
    public const int SpatialLongitude = 3;
    public const int SpatialUnits = 4;
    // Spatial WKT: [0]=distErrPct, [1]=wkt, [2]=units
    public const int SpatialWkt = 1;
    public const int SpatialWktUnits = 2;

    // Vector: [0]=vectorValue, [1]=minimumMatch, [2]=numberOfCandidates, [3]=aiTaskName
    public const int VectorValue = 0;
    public const int VectorMinMatch = 1;
    public const int VectorCandidates = 2;
    public const int VectorAiTask = 3;

    // IN/AllIn: [0..N] = each term binding (array params expand at resolution time)
}
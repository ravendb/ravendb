namespace Corax.Querying.Matches.Meta;

public interface IBoostingMarker
{
}

public struct HasBoosting : IBoostingMarker
{
}

public struct HasBoostingNoStore : IBoostingMarker
{
}

public struct NoBoosting : IBoostingMarker
{
    
}

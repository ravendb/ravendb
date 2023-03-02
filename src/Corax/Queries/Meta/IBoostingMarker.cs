namespace Corax.Queries;

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

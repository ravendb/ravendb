namespace Corax.Querying.Planning;

public enum ClauseType : byte
{
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Between,
    In,
    AllIn,
    Exists,
    StartsWith,
    EndsWith,
    Search,
    Regex,
    Spatial,
    Vector,
    OrGroup,  // A group of OR'd subclauses
    AndGroup, // A group of AND'd subclauses inside an OR chain

    // Eliminated clause filter ( via WHEN() ), so it matches everything 
    MatchAll,
    // This clause is known to have no matches (contradictory BETWEEN, empty IN, etc)
    MatchNothing,
}

namespace Corax.Querying.Planning;

public enum MatchDispatch : byte
{
    /// <summary>IQueryMatch.Fill() dispatch — vector / search / boosted, etc.</summary>
    QueryMatch,

    /// <summary>Native posting-list dispatch — a single resolved posting list. Used for Equals and NotEquals clauses.</summary>
    PostingList,

    /// <summary>CompactTree scan — iterates the tree. Used for StartsWith, EndsWith, Contains, Exists, Regex, and range clauses.</summary>
    TreeScan,
}

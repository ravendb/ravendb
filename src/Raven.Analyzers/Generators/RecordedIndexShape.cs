namespace Raven.Analyzers.Generators
{
    /// <summary>
    /// One index's shape, reduced to the values that go into its attribute.
    /// </summary>
    /// <remarks>
    /// Every member is a string, bool or int, so the compiler-generated record equality really does
    /// compare contents. That is the whole point of this type: it is what travels through the generator
    /// pipeline, and an incremental generator can only reuse a cached step when its value compares equal
    /// to the previous run. A symbol would compare by reference and pin the compilation it came from, so
    /// nothing downstream could ever be reused. Field lists arrive already rendered as the comma-joined,
    /// quoted contents of a <c>new string[] { … }</c>, which keeps them comparable as plain strings and
    /// leaves the emit step doing nothing but concatenation.
    /// </remarks>
    internal sealed record RecordedIndexShape(
        string TypeName,
        bool Analyzable,
        string MapFields,
        string StoredFields,
        bool StoreAllFields,
        bool AssignsMap,
        int AddMapCount,
        bool AddMapInLoop,
        bool UsesAdditionalCode);
}

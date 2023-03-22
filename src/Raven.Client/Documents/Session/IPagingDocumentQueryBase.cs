namespace Raven.Client.Documents.Session;

public interface IPagingDocumentQueryBase<T, out TSelf>
    where TSelf : IPagingDocumentQueryBase<T, TSelf>
{
    /// <summary>
    ///     Skips the specified count.
    /// </summary>
    /// <param name="count">Number of items to skip.</param>
    TSelf Skip(long count);

    /// <summary>
    ///     Takes the specified count.
    /// </summary>
    /// <param name="count">Maximum number of items to take.</param>
    TSelf Take(long count);
}

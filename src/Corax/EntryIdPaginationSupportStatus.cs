namespace Corax;

/// <summary>
/// This allows us to determine if we can perform pagination based on Corax's entry IDs instead of raw IDs in certain cases.
/// If we have a newer index with this capability, and the newer index contains only unique IDs, we can perform paging (eliminating duplicate IDs) by creating a hashmap of Corax's internal entry IDs.
/// </summary>
public enum EntryIdPaginationSupportStatus : long
{
    Unknown = 0L,
    Supported = 1L,
    Disabled = 1L << 1,
}

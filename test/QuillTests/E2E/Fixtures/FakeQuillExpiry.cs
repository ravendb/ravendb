using Raven.Quill.Hosting;

namespace QuillTests.E2E.Fixtures;

/// The verdict a build carries, injected rather than derived, so a test picks the side of the window it
/// wants and every other test stays on a live build no matter how stale the compiled release date is.
internal sealed class FakeQuillExpiry(bool isExpired, DateTime expiresAt) : IQuillExpiry
{
    public bool IsExpired { get; } = isExpired;
    public DateTime ExpiresAt { get; } = expiresAt;
}

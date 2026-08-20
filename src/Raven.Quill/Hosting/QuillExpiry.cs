using Raven.Client.Properties;

namespace Raven.Quill.Hosting;

public interface IQuillExpiry
{
    bool IsExpired { get; }
    DateTime ExpiresAt { get; }
}

public sealed class QuillExpiry : IQuillExpiry
{
    internal const int Days = 90;

    public QuillExpiry() : this(DateTime.UtcNow)
    {
    }

    public QuillExpiry(DateTime utcNow)
    {
        ExpiresAt = RavenVersionAttribute.Instance.ReleaseDate.AddDays(Days);
        IsExpired = utcNow > ExpiresAt;
    }

    public bool IsExpired { get; }
    public DateTime ExpiresAt { get; }
}

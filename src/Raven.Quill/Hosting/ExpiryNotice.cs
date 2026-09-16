namespace Raven.Quill.Hosting;

public sealed class ExpiryNotice
{
    /// Raven.Quill.Web builds it as a single fully inlined file.
    /// `build:expired` script emits dist/expired.html, which the image copies into wwwroot
    internal const string FileRelativePath = "expired.html";

    private ExpiryNotice(string page)
    {
        Page = page;
    }

    public string Page { get; }

    public static ExpiryNotice FromHtml(string html) => new(html);

    public static ExpiryNotice Load(IWebHostEnvironment environment)
    {
        var webRoot = environment.WebRootPath;
        var noticePath = string.IsNullOrEmpty(webRoot) ? null : Path.Combine(webRoot, FileRelativePath);
        if (noticePath is null || File.Exists(noticePath) == false)
        {
            throw new InvalidOperationException(
                $"The expiry notice was not found at '{noticePath ?? FileRelativePath}'. " +
                "Raven.Quill.Web's `pnpm build` emits it as dist/expired.html, which the image copies into wwwroot.");
        }

        return new ExpiryNotice(File.ReadAllText(noticePath));
    }
}

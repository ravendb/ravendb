using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

namespace Raven.Quill.Embed;

/// Resolves the widget bundle's hashed asset URLs from the Vite manifest the widget build emits into
/// `wwwroot/widget/.vite/manifest.json`. Read once at startup: the manifest only changes when the image
/// is rebuilt, and re-reading it per embed request would put file IO on the public hot path.
public sealed class WidgetAssets
{
    internal const string ManifestRelativePath = "widget/.vite/manifest.json";
    private const string UrlPrefix = "/widget/";

    private WidgetAssets(string? scriptUrl, string[] styleUrls, string[] moduleUrls)
    {
        ScriptUrl = scriptUrl;
        StyleUrls = styleUrls;
        ModuleUrls = moduleUrls;
    }

    /// Null when the manifest is missing or unreadable; the embed route turns that into a 503 rather than
    /// serving a shell that would render a blank frame.
    public string? ScriptUrl { get; }

    public string[] StyleUrls { get; }

    /// The entry's static imports, emitted as `<link rel="modulepreload">` so the browser fetches the
    /// whole initial graph in parallel instead of discovering it after parsing the entry.
    public string[] ModuleUrls { get; }

    public bool IsAvailable => ScriptUrl is not null;

    public static WidgetAssets Unavailable { get; } = new(null, [], []);

    public static WidgetAssets Load(IWebHostEnvironment environment, ILogger logger)
    {
        var webRoot = environment.WebRootPath;
        if (string.IsNullOrEmpty(webRoot))
        {
            logger.LogError("no web root configured; the embeddable widget cannot be served");
            return Unavailable;
        }

        var manifestPath = Path.Combine(webRoot, ManifestRelativePath.Replace('/', Path.DirectorySeparatorChar));
        if (File.Exists(manifestPath) == false)
        {
            logger.LogError(
                "widget asset manifest not found at {ManifestPath}; embed pages will return 503 until the widget bundle is built into wwwroot/widget",
                manifestPath);
            return Unavailable;
        }

        try
        {
            return FromManifestJson(File.ReadAllText(manifestPath), logger);
        }
        catch (Exception e)
        {
            logger.LogError(e, "could not read the widget asset manifest at {ManifestPath}", manifestPath);
            return Unavailable;
        }
    }

    public static WidgetAssets FromManifestJson(string json, ILogger logger)
    {
        var manifest = JsonSerializer.Deserialize<Manifest>(json, ManifestJsonOptions);
        var entry = manifest?.Values.FirstOrDefault(chunk => chunk.IsEntry);
        if (manifest is null || entry?.File is null)
        {
            logger.LogError("the widget asset manifest has no entry chunk");
            return Unavailable;
        }

        return new WidgetAssets(
            scriptUrl: UrlPrefix + entry.File,
            styleUrls: CollectStyles(manifest, entry).Select(file => UrlPrefix + file).ToArray(),
            moduleUrls: CollectImports(manifest, entry).Select(file => UrlPrefix + file).ToArray());
    }

    /// A chunk's CSS lives on whichever chunk pulled it in, so the entry's own `css` isn't enough - the
    /// statically imported chunks have to be walked too.
    private static List<string> CollectStyles(Manifest manifest, ManifestChunk entry)
    {
        var styles = new List<string>();
        foreach (var chunk in WalkGraph(manifest, entry))
        {
            foreach (var css in chunk.Css)
            {
                if (styles.Contains(css) == false)
                    styles.Add(css);
            }
        }

        return styles;
    }

    private static List<string> CollectImports(Manifest manifest, ManifestChunk entry) =>
        WalkGraph(manifest, entry)
            .Where(chunk => ReferenceEquals(chunk, entry) == false && chunk.File is not null)
            .Select(chunk => chunk.File!)
            .Distinct()
            .ToList();

    private static List<ManifestChunk> WalkGraph(Manifest manifest, ManifestChunk entry)
    {
        var visited = new List<ManifestChunk>();
        var pending = new Stack<ManifestChunk>();
        pending.Push(entry);

        while (pending.Count > 0)
        {
            var chunk = pending.Pop();
            if (visited.Contains(chunk))
                continue;

            visited.Add(chunk);
            foreach (var key in chunk.Imports)
            {
                if (manifest.TryGetValue(key, out var imported))
                    pending.Push(imported);
            }
        }

        return visited;
    }

    private static readonly JsonSerializerOptions ManifestJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private sealed class Manifest : Dictionary<string, ManifestChunk>;

    private sealed class ManifestChunk
    {
        [JsonPropertyName("file")]
        public string? File { get; set; }

        [JsonPropertyName("isEntry")]
        public bool IsEntry { get; set; }

        [JsonPropertyName("css")]
        public string[] Css { get; set; } = [];

        [JsonPropertyName("imports")]
        public string[] Imports { get; set; } = [];
    }
}

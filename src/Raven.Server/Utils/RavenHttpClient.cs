using System.Globalization;
using System.Net.Http;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;

namespace Raven.Server.Utils;

public sealed class RavenHttpClient : HttpClient
{
    private static readonly string UserAgent = $"RavenDB/{ServerVersion.Version} (" +
                                               $"{RuntimeInformation.OSDescription};" +
                                               $"{RuntimeInformation.OSArchitecture};" +
                                               $"{RuntimeInformation.FrameworkDescription};" +
                                               $"{RuntimeInformation.ProcessArchitecture};" +
                                               $"{CultureInfo.CurrentCulture.Name};" +
                                               $"{CultureInfo.CurrentUICulture.Name};" +
                                               $"{ServerVersion.FullVersion})";

    public RavenHttpClient()
    {
        AddUserAgentHeader();
    }

    public RavenHttpClient(HttpMessageHandler handler)
        : base(handler)
    {
        AddUserAgentHeader();
    }

    public RavenHttpClient(HttpMessageHandler handler, bool disposeHandler)
        : base(handler, disposeHandler)
    {
        AddUserAgentHeader();
    }

    private void AddUserAgentHeader()
    {
        DefaultRequestHeaders.Add("User-Agent", UserAgent);
    }
}

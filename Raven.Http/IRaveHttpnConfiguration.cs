using System.Collections.Specialized;
using System.ComponentModel.Composition.Hosting;

namespace Raven.Database.Server.Responders
{
    public interface IRaveHttpnConfiguration
    {
        string VirtualDirectory { get; }
        AnonymousUserAccessMode AnonymousUserAccessMode { get; }
        string HostName { get; }
        int Port { get; }
        CompositionContainer Container { get; set; }
        bool HttpCompression { get; }
        string WebDir { get; }
        string AccessControlAllowOrigin { get; }
        NameValueCollection Settings { get; }
        string GetFullUrl(string url);
    }
}
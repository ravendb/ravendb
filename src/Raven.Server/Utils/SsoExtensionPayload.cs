using Raven.Client.ServerWide.Operations.Certificates;

namespace Raven.Server.Utils
{
    public readonly struct SsoExtensionPayload
    {
        public readonly string Username;
        public readonly SsoProvider Provider;
        public readonly string Domain;

        public SsoExtensionPayload(string username, SsoProvider provider, string domain = null)
        {
            Username = username;
            Provider = provider;
            Domain = domain;
        }

        public bool IsEmpty => string.IsNullOrEmpty(Username);

        public string GetDisplayIdentity()
        {
            if (Provider == SsoProvider.Windows && string.IsNullOrEmpty(Domain) == false)
                return $"Windows\\{Domain}:{Username}";
            return $"{Provider}:{Username}";
        }
    }
}

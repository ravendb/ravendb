using Raven.Json.Linq;

namespace Raven.Client
{
    public interface ILoadConfiguration
    {
        void AddQueryParam(string name, RavenJToken value);
    }
}
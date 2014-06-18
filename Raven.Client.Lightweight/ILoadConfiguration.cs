using Raven.Json.Linq;

namespace Raven.Client
{
    public interface ILoadConfiguration
    {
        void AddTransformerParameter(string name, RavenJToken value);
    }
}
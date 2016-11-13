using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client
{
    public delegate bool AfterStreamExecutedDelegate(ref RavenJObject document);
}

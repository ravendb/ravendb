using Raven.Json.Linq;

namespace Raven.Client
{
    public delegate bool AfterStreamExecutedDelegate(ref RavenJObject document);
}
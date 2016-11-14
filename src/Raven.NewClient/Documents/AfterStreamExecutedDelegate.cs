using Raven.NewClient.Client.Documents;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Documents
{
    public delegate bool AfterStreamExecutedDelegate(ref StreamResult document);
}

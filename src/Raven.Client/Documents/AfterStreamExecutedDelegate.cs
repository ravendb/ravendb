using Raven.Client.Documents;
using Raven.Json.Linq;

namespace Raven.Client.Documents
{
    public delegate bool AfterStreamExecutedDelegate(ref StreamResult document);
}

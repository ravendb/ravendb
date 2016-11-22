using Raven.NewClient.Client.Commands;

namespace Raven.NewClient.Client.Document
{
    public delegate bool AfterStreamExecutedDelegate(ref StreamResult document);
}

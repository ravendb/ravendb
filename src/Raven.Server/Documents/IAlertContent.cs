using System.Security.Cryptography.X509Certificates;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public interface IAlertContent
    {
        DynamicJsonValue ToJson();
    }
}
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public sealed class ClientCertificateGenerationResult : IOperationResult
    {
        public static readonly ClientCertificateGenerationResult Instance = new ClientCertificateGenerationResult();

        private ClientCertificateGenerationResult()
        {
        }

        public string Message => "Client certificate was generated.";

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Message"] = Message
            };
        }

        public bool ShouldPersist => false;
    }
}

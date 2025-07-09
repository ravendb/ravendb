using Raven.Server.Commercial;

namespace Raven.Server.Documents.AI.AiAssistant;

public class AiAssistantRequestAuthentication
{
    public License License { get; set; }
    
    public string CertificateThumbprint { get; set; }
}

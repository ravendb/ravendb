using Raven.Server.Commercial;

namespace Raven.Server.Documents.AI.AiAssistant.Requests;

public class AiAssistantRequestAuthentication
{
    public License License { get; set; }
    
    public string CertificateThumbprint { get; set; }
}

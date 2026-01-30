
namespace Raven.Server.Documents.SchemaValidation;

public class SchemaBuilderContext
{
    public RefSchemas RefSchemas { init; get; }
    
    public SchemaValidatorSettings Configuration { get; set; }
}


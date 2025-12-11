using Raven.Server.Documents.SchemaValidation.Validators;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation;

public class RefSchema
{
    public ElementSchemaRuleValidator Validator { get; set; } 
    public BlittableJsonReaderObject Raw { get; set; }
}

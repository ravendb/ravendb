using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.Documents.SchemaValidation.Validators;
using Raven.Server.Documents.SchemaValidation.Validators.String;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation;

[DebuggerDisplay("{SchemaDefinition}")]
public class SchemaValidator
{
    private ElementSchemaRuleValidator _root;

    private SchemaValidatorSettings _configuration;

    public string SchemaDefinition { get; init; }

    public bool Disabled { get; set; }

    public static void ValidateInit(BlittableJsonReaderObject schemaDefinition)
    {
        //To make sure the schema is valid, we run Init on an instance of SchemaValidator, but it is not used
        new SchemaValidator().Init(schemaDefinition, new SchemaValidatorSettings{RegexTimeout = Regex.InfiniteMatchTimeout});
    }

    // The caller (holder) is responsible for keeping the underlying Blittable / context alive (and disposing it) for as long as this SchemaValidator is used.
    public void Init(BlittableJsonReaderObject schemaDefinition, SchemaValidatorSettings configuration)
    {
        _configuration = configuration;
        var context = new SchemaBuilderContext
        {
            RefSchemas = new RefSchemas(),
            Configuration = _configuration
        };
        
        context.RefSchemas.Init(context, schemaDefinition);

        _root = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(context, schemaDefinition, new SchemaPath());
    }
    
    public bool Validate(BlittableJsonReaderObject obj, ErrorBuilder errorBuilder)
    {
        var context = new SchemaValidationContext(_configuration)
        {
            ErrorBuilder = errorBuilder,
        };
        
        return _root.Validate(context, obj);
    }
}

public class SchemaValidatorSettings
{
    public TimeSpan RegexTimeout { get; init; }
    public TimeSpan ValidationTimeout { get; init; }
    public int MaxDepth { get; init; }
}

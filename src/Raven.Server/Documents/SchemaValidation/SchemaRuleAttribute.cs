using System;

namespace Raven.Server.Documents.SchemaValidation;

internal class SchemaRuleAttribute : Attribute
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public SchemaRuleAttribute(string ruleProp)
    {
        Rule = ruleProp;
    }

    public string Rule { get; }
}

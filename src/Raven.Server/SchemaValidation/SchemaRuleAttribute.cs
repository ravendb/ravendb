using System;

namespace Raven.Server.SchemaValidation;

internal class SchemaRuleAttribute(string ruleProp, params string[] additionalInfoProps) : Attribute
{
    public string Rule { get; } = ruleProp;
    public string[] AdditionalInfoProps { get; } = additionalInfoProps;
}

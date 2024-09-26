using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

//TODO Maybe change the name
internal class IntegerSchemaRuleValidator : PropertySchemaRuleValidator<long>
{
    public static readonly Dictionary<string, (Type x, string[] AdditionalInfoProps)> LongValuesSchemaRuleValidator = Assembly.GetExecutingAssembly().GetTypes()
        .Where(t => typeof(SchemaRuleValidator<long>).IsAssignableFrom(t) && !t.IsAbstract && CustomAttributeExtensions.GetCustomAttribute<SchemaRuleAttribute>((MemberInfo)t) != null)
        .ToDictionary(x => x.GetCustomAttribute<SchemaRuleAttribute>()?.Rule, x => (x, x.GetCustomAttribute<SchemaRuleAttribute>()?.AdditionalInfoProps));

    // ReSharper disable once ConvertToPrimaryConstructor
    public IntegerSchemaRuleValidator(string path, string property, bool isRequired) : base(path, property, isRequired)
    {
    }

    protected override bool IsOfRequiredType(BlittableJsonToken token) => (token & BlittableJsonReaderBase.TypesMask) == BlittableJsonToken.Integer;
    
    public override void ReadSchemaDefinition(BlittableJsonReaderObject propertySchemaDefinition)
    {
        var rulesValidators = new List<ISchemaRuleValidator<long>>();
        rulesValidators.Add(new IntegerRangeSchemaRuleValidator(Path));
        ReadValueSchemaRuleValidators(propertySchemaDefinition, Path, rulesValidators);
        RuleValidators = rulesValidators.ToArray();
    }
}

internal class IntegerRangeSchemaRuleValidator(string path) : SchemaRuleValidator<long>(path)
{
    public override void Validate(long value, StringBuilder errorBuilder)
    {
        // ReSharper disable once ArrangeRedundantParentheses
        if ((value is >= int.MinValue and <= int.MaxValue) == false)
            errorBuilder.AppendLine($"the value {value} should be in the range of integer");
    }
}

[SchemaRule("minimum", "exclusiveMinimum")]
internal class MinimumSchemaRuleValidator : SchemaRuleValidator<long>
{
    private readonly long _minimum;
    private readonly Action<long, StringBuilder> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MinimumSchemaRuleValidator(string path, long minimum) : this(path, minimum, false)
    {
    }
    
    public MinimumSchemaRuleValidator(string path, long minimum, bool exclusiveMinimum) : base(path)
    {
        _minimum = minimum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidateInternal : ValidateInternal;
    }

    public override void Validate(long value, StringBuilder errorBuilder) => _validatePredicate(value, errorBuilder);

    private void ValidateInternal(long value, StringBuilder errorBuilder)
    {
        if(value >= _minimum == false)
            errorBuilder.AppendLine($"the value {value} should be greater or equal to {_minimum}");
    }
    private void ExclusiveValidateInternal(long value, StringBuilder errorBuilder)
    {
        if(value > _minimum == false)
            errorBuilder.AppendLine($"the value {value} should be greater then {_minimum}");
    }
}


[SchemaRule("maximum", "exclusiveMaximum")]
internal class MaximumSchemaRuleValidator : SchemaRuleValidator<long>
{
    private readonly long _maximum;
    private readonly Action<long, StringBuilder> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MaximumSchemaRuleValidator(string path, long maximum) : this(path, maximum, false)
    {
    }
    
    public MaximumSchemaRuleValidator(string path, long maximum, bool exclusiveMinimum) : base(path)
    {
        _maximum = maximum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidateInternal : ValidateInternal;
    }

    public override void Validate(long value, StringBuilder errorBuilder) => _validatePredicate(value, errorBuilder);

    private void ValidateInternal(long value, StringBuilder errorBuilder)
    {
        if(value <= _maximum == false)
            errorBuilder.AppendLine($"the value {value} should be smaller or equal to {_maximum}");
    }
    private void ExclusiveValidateInternal(long value, StringBuilder errorBuilder)
    {
        if(value < _maximum == false)
            errorBuilder.AppendLine($"the value {value} should be smaller then {_maximum}");
    }
}

internal class SchemaRuleAttribute(string ruleProp, params string[] additionalInfoProps) : Attribute
{
    public string Rule { get; } = ruleProp;
    public string[] AdditionalInfoProps { get; } = additionalInfoProps;
}

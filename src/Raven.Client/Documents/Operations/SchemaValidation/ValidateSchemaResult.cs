using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public class ValidateSchemaValidationProgress : IOperationProgress
{
    public long ErrorCount { get; set; }
    public long ValidatedCount { get; set; }

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue(GetType())
        {
            [nameof(ErrorCount)] = ErrorCount,
            [nameof(ValidatedCount)] = ValidatedCount,
        };
    }

    public IOperationProgress Clone()
    {
        return new ValidateSchemaValidationProgress
        {
            ErrorCount = ErrorCount,
            ValidatedCount = ValidatedCount,
        };
    }

    public bool CanMerge => true;
    public void MergeWith(IOperationProgress progress)
    {
        if (progress is not ValidateSchemaValidationProgress p)
            return;
        
        ErrorCount += p.ErrorCount;
        ValidatedCount += p.ValidatedCount;
    }
}

public sealed class ValidateSchemaResult : ValidateSchemaValidationProgress, IOperationResult
{
    public Dictionary<string, string> Errors { get; set; }
    
    public long LastEtag { get; set; }
    
    public override DynamicJsonValue ToJson()
    {
        var errors = new DynamicJsonValue();
        foreach (var keyValue in Errors)
        {
            errors[keyValue.Key] = keyValue.Value;
        }

        var result = base.ToJson();
        result[nameof(Errors)] = errors;
        result[nameof(LastEtag)] = LastEtag;
        return  result;
    }
    
    string IOperationResult.Message { get; }
    
    public bool ShouldPersist => false;

    bool IOperationResult.CanMerge => true;

    void IOperationResult.MergeWith(IOperationResult result)
    {
        if (result is not ValidateSchemaResult r)
            return;
        ErrorCount += r.ErrorCount;
        ValidatedCount += r.ValidatedCount;

        Errors ??= new Dictionary<string, string>();
        foreach (var keyValue in r.Errors)
        {
            Errors[keyValue.Key] = keyValue.Value;
        }
    }
}

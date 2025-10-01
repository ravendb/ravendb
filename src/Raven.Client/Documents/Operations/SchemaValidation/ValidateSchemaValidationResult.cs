using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public class ValidateSchemaValidationProgress : IOperationProgress
{
    public int ErrorCount { get; set; }
    public int ScannedCount { get; set; }

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue(GetType())
        {
            [nameof(ErrorCount)] = ErrorCount,
            [nameof(ScannedCount)] = ScannedCount,
        };
    }

    public IOperationProgress Clone()
    {
        return new ValidateSchemaValidationProgress
        {
            ErrorCount = ErrorCount,
            ScannedCount = ScannedCount,
        };
    }

    public bool CanMerge => true;
    public void MergeWith(IOperationProgress progress)
    {
        if (progress is not ValidateSchemaValidationProgress p)
            return;
        
        ErrorCount += p.ErrorCount;
        ScannedCount += p.ScannedCount;
    }
}

public sealed class ValidateSchemaValidationResult : ValidateSchemaValidationProgress, IOperationResult
{
    public Dictionary<string, string> Errors { get; set; }
    
    public override DynamicJsonValue ToJson()
    {
        var errors = new DynamicJsonValue();
        foreach (var keyValue in Errors)
        {
            errors[keyValue.Key] = keyValue.Value;
        }

        var result = base.ToJson();
        result[nameof(Errors)] = errors;
        return  result;
    }
    
    string IOperationResult.Message { get; }
    
    public bool ShouldPersist => false;

    bool IOperationResult.CanMerge => true;

    void IOperationResult.MergeWith(IOperationResult result)
    {
        if (result is not ValidateSchemaValidationResult r)
            return;
        ErrorCount += r.ErrorCount;
        ScannedCount += r.ScannedCount;

        Errors ??= new Dictionary<string, string>();
        foreach (var keyValue in r.Errors)
        {
            Errors[keyValue.Key] = keyValue.Value;
        }
    }
}

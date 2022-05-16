using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations;

public class OperationMultipleExceptionsResult : IOperationResult
{
    public OperationMultipleExceptionsResult()
    {
        
    }

    public OperationMultipleExceptionsResult(string message)
    {
        Message = message;
    }

    public string Message { get; }

    public List<OperationExceptionResult> Exceptions { get; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue()
        {
            [nameof(Message)] = Message,
            [nameof(Exceptions)] = new DynamicJsonArray(Exceptions.Select(x => x.ToJson()))
        };
    }

    public bool ShouldPersist { get; }

    bool IOperationResult.CanMerge => false;

    void IOperationResult.MergeWith(IOperationResult result)
    {
        throw new System.NotImplementedException();
    }
}

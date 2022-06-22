using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueWriterSimulator
{
    public List<MessageSummary> SimulateExecuteMessages<T>(QueueWithItems<T> queueMessages, DocumentsOperationContext context)
        where T : QueueItem
    {
        List<MessageSummary> result = new();
        if (queueMessages.Items.Count <= 0) return result;

        
        foreach (var message in queueMessages.Items)
        {
            var messageSummary = new MessageSummary()
            {
                Body = message.TransformationResult.ToString(),
                Attributes = message.Attributes
            };
            
            result.Add(messageSummary);
        }
        
        return result;
    }
}

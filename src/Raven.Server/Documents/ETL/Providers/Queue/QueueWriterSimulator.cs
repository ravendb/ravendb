using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueWriterSimulator
{
    public List<MessageSummary> SimulateExecuteMessages(QueueWithMessages queueMessages, DocumentsOperationContext context)
    {
        List<MessageSummary> result = new();
        if (queueMessages.Messages.Count <= 0) return result;

        
        foreach (var message in queueMessages.Messages)
        {
            var messageSummary = new MessageSummary()
            {
                Body = message.TransformationResult.ToString(),
                Options = message.Options
            };
            
            result.Add(messageSummary);
        }
        
        return result;
    }
}

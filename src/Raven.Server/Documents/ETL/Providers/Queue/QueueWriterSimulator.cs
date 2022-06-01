using System.Collections.Generic;
using System.Text;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueWriterSimulator
{
    public IEnumerable<string> SimulateExecuteCommandText(QueueWithMessages records, DocumentsOperationContext context)
    {
        List<string> result = new List<string>();

        result.AddRange(GenerateInsertKafkaMessageCommandText(records.Name, records, context));

        return result;
    }

    private IEnumerable<string> GenerateInsertKafkaMessageCommandText(string topicName, QueueWithMessages topic,
        DocumentsOperationContext context)
    {
        List<string> result = new();

        if (topic.Messages.Count <= 0) return result;
        StringBuilder sb = new();

        foreach (QueueItem item in topic.Messages) sb.AppendLine(item.TransformationResult.ToString());

        result.Add(sb.ToString());

        return result;
    }
}

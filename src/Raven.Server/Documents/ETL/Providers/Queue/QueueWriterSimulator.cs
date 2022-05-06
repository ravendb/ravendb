using System.Collections.Generic;
using System.Text;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueWriterSimulator
{
    public IEnumerable<string> SimulateExecuteCommandText(QueueWithEvents records, DocumentsOperationContext context)
    {
        List<string> result = new List<string>();

        result.AddRange(GenerateInsertKafkaMessageCommandText(records.Name, records, context));

        return result;
    }

    private IEnumerable<string> GenerateInsertKafkaMessageCommandText(string topicName, QueueWithEvents topic,
        DocumentsOperationContext context)
    {
        List<string> result = new();

        if (topic.Inserts.Count <= 0) return result;
        StringBuilder sb = new();

        foreach (QueueItem item in topic.Inserts) sb.AppendLine(item.TransformationResult.ToString());

        result.Add(sb.ToString());

        return result;
    }
}

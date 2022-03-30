using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;

namespace Raven.Server.Documents.Handlers.Batches;

public interface IBatchCommandData
{
    CommandType Type { get; }

    string Id { get;  }

    MergedBatchCommand.AttachmentStream AttachmentStream { get; set; }
}

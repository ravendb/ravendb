using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static.Attachments;

public interface IAttachmentIndexObject : IAttachmentObjectBase
{
    public dynamic RetireAt { get; }

    public RetiredAttachmentFlags RetireFlags { get; }

    public string RetireIdentifier { get; }
}

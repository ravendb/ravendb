using Corax.Queries;

namespace Raven.Server.Documents.Queries.MoreLikeThis.Corax;

public sealed class MoreLikeThisQuery : MoreLikeThisQueryBase
{
    public IQueryMatch BaseDocumentQuery { get; set; }

    public IQueryMatch FilterQuery { get; set; }
}

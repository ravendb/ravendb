using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.AI;

public class AiStorage
{
    private readonly DocumentsStorage _documentsStorage;

    public AiStorage([NotNull] DocumentsStorage documentsStorage)
    {
        _documentsStorage = documentsStorage ?? throw new ArgumentNullException(nameof(documentsStorage));
    }

    public ValueEmbeddingsDocument GetValueEmbeddingsDocument(DocumentsOperationContext context, AiEtlConfiguration configuration, string value, out string valueEmbeddingsDocumentId)
    {
        valueEmbeddingsDocumentId = AiHelper.ValueEmbeddingsDocumentId(configuration.Name, AiHelper.CalculateValueHash(value));

        var document = _documentsStorage.Get(context, valueEmbeddingsDocumentId);
        if (document == null)
            return null;

        return new ValueEmbeddingsDocument(document);
    }
}

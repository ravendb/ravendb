using System;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

public class EmbeddingGenerationException(string msg, Exception innerException) : Exception(msg, innerException);

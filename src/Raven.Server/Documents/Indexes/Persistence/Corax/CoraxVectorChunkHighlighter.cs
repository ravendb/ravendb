using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Highlightings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Hnsw = Voron.Data.Graphs.Hnsw;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal sealed class VectorChunkHighlightingCapture
{
    public readonly string FieldName;
    public readonly string TaskId;
    public readonly List<byte[]> QueryVectors;

    public float MinimumSimilarity { get; private set; }

    public VectorChunkHighlightingCapture(string fieldName, string taskId, float minimumSimilarity, List<byte[]> queryVectors)
    {
        FieldName = fieldName;
        TaskId = taskId;
        MinimumSimilarity = minimumSimilarity;
        QueryVectors = queryVectors;
    }

    public void RelaxMinimumSimilarity(float minimumSimilarity)
    {
        if (minimumSimilarity < MinimumSimilarity)
            MinimumSimilarity = minimumSimilarity;
    }
}

internal static class CoraxVectorChunkHighlighter
{
    public static void Apply(
        Dictionary<string, Dictionary<string, string[]>> highlightings,
        IReadOnlyDictionary<string, VectorChunkHighlightingCapture> captures,
        IndexQueryServerSide query,
        Document document,
        DocumentsOperationContext context,
        DocumentDatabase database)
    {
        if (document?.Id == null || query.Metadata.Highlightings == null)
            return;

        PriorityQueue<string, float> scored = new();

        foreach (HighlightingField highlighting in query.Metadata.Highlightings)
        {
            string fieldName = highlighting.Field.Value;
            if (captures.TryGetValue(fieldName, out VectorChunkHighlightingCapture capture) == false)
                continue; // this highlight field was not a vector search

            scored.Clear();
            string[] fragments = ComputeForDocument(scored, capture, highlighting, document, context, database);
            if (fragments is not { Length: > 0 })
                continue;

            if (highlightings.TryGetValue(fieldName, out Dictionary<string, string[]> perDocument) == false)
                highlightings[fieldName] = perDocument = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

            // On a dynamic index the same source field can be both full-text searched and vector searched under a single
            // highlight(), in which case the term highlighter has already stored its fragments here. Append the vector
            // chunk fragments rather than overwriting so both survive.
            if (perDocument.TryGetValue(document.Id, out string[] existing) && existing is { Length: > 0 })
            {
                int offset = existing.Length;
                Array.Resize(ref existing, existing.Length + fragments.Length);
                Array.Copy(fragments, 0, existing, offset, fragments.Length);
                perDocument[document.Id] = existing;
            }
            else
            {
                perDocument[document.Id] = fragments;
            }
        }
    }

    private static string[] ComputeForDocument(PriorityQueue<string, float> scored, VectorChunkHighlightingCapture capture, HighlightingField highlighting, Document document,
        DocumentsOperationContext context, DocumentDatabase database)
    {
        string embeddingDocumentId = EmbeddingsHelper.GetEmbeddingDocumentId(document.Id);

        using Document embeddingDocument = database.DocumentsStorage.Get(context, embeddingDocumentId);
        if (embeddingDocument == null)
            return null; // no embeddings stored for this document (e.g. deleted, manual vector, etc)

        if (embeddingDocument.Data.TryGet(capture.TaskId, out BlittableJsonReaderObject taskObject) == false || taskObject == null)
            return null; // this task produced no embeddings for the document

        if (taskObject.TryGet(EmbeddingsHelper.ChunkTextPropertyName, out BlittableJsonReaderObject chunkTextByHash) == false || chunkTextByHash == null)
            return null; // chunk text was not stored (StoreChunkText disabled, or data predates the feature)

        VectorEmbeddingType quantization = VectorEmbeddingType.Single;
        if (taskObject.TryGet(Constants.Documents.Metadata.Quantization, out string quantizationValue) &&
            Enum.TryParse(quantizationValue, out VectorEmbeddingType parsedQuantization))
            quantization = parsedQuantization;

        if (TryGetSimilarityMethod(quantization, out Hnsw.SimilarityMethod similarityMethod) == false)
            return null; // not a quantization that vectors are stored in, so there is nothing to compare the chunks against

        AttachmentsStorage attachmentsStorage = database.DocumentsStorage.AttachmentsStorage;

        // Keep only the nearest FragmentCount chunks (all of them when FragmentCount <= 0). Priorities are the negated
        // distance, so the default min-heap keeps the farthest kept chunk at the head - the one EnqueueDequeue evicts.
        int capacity = highlighting.FragmentCount > 0 ? highlighting.FragmentCount : int.MaxValue;

        // Text field is a map of chunk hash -> chunk text, vector data stored as attachment
        BlittableJsonReaderObject.PropertyDetails property = default;
        for (int i = 0; i < chunkTextByHash.Count; i++)
        {
            chunkTextByHash.GetPropertyByIndex(i, ref property);
            string hash = property.Name?.ToString();
            if (hash == null)
                continue;

            string text = property.Value?.ToString();
            if (text == null)
                continue; // no text stored for this chunk

            Attachment attachment = attachmentsStorage.GetAttachment(context, embeddingDocumentId, hash, AttachmentType.Document, changeVector: null);
            if (attachment == null)
                continue; // vector attachment is gone

            // A chunk is only comparable to a query vector of the same length (same dimensions and quantization), so an
            // attachment that matches none of them cannot match at all - skip it before allocating and reading it.
            if (HasQueryVectorOfSize(capture.QueryVectors, attachment.Size) == false)
                continue;

            float distance;
            using (context.Allocator.Allocate((int)attachment.Size, out Span<byte> chunkVector))
            {
                attachment.Stream.ReadExactly(chunkVector);
                distance = BestDistance(capture.QueryVectors, chunkVector, similarityMethod);

                // A document matches when any one of its chunks is near enough, so the rest of its chunks can be
                // arbitrarily far from the query - without this the highlighter would return every stored chunk.
                if (float.IsNaN(distance) ||
                    distance > Hnsw.MinimumSimilarityToDistance(similarityMethod, chunkVector.Length, capture.MinimumSimilarity))
                    continue; // below the query's minimum similarity, or a degenerate vector
            }

            if (scored.Count < capacity)
                scored.Enqueue(text, -distance);
            else
                scored.EnqueueDequeue(text, -distance); // full: adds this chunk and drops the current farthest
        }

        int count = scored.Count;
        if (count == 0)
            return null;

        // The queue dequeues farthest-first, fill in reverse
        string[] result = new string[count];
        for (int i = count - 1; i >= 0; i--)
        {
            string text = scored.Dequeue();
            if (highlighting.FragmentLength > 0 && text.Length > highlighting.FragmentLength)
                text = text.Substring(0, highlighting.FragmentLength);
            result[i] = text;
        }

        return result;
    }

    private static bool HasQueryVectorOfSize(List<byte[]> queryVectors, long size)
    {
        foreach (byte[] queryVector in queryVectors)
        {
            if (queryVector.Length == size)
                return true;
        }

        return false;
    }

    // The quantization the chunks were stored with decides both the distance kernel and how a minimum similarity
    // translates into a maximum distance, so it is mapped to Hnsw's own notion of that once, here.
    private static bool TryGetSimilarityMethod(VectorEmbeddingType quantization, out Hnsw.SimilarityMethod similarityMethod)
    {
        switch (quantization)
        {
            case VectorEmbeddingType.Single:
                similarityMethod = Hnsw.SimilarityMethod.CosineSimilaritySingles;
                return true;
            case VectorEmbeddingType.Int8:
                similarityMethod = Hnsw.SimilarityMethod.CosineSimilarityI8;
                return true;
            case VectorEmbeddingType.Binary:
                similarityMethod = Hnsw.SimilarityMethod.HammingDistance;
                return true;
            default:
                similarityMethod = default; // Text is a source type only, vectors are never stored that way
                return false;
        }
    }

    private static float BestDistance(List<byte[]> queryVectors, ReadOnlySpan<byte> chunkVector, Hnsw.SimilarityMethod similarityMethod)
    {
        float best = float.PositiveInfinity;
        foreach (byte[] queryVector in queryVectors)
        {
            float distance = Distance(similarityMethod, queryVector, chunkVector);
            if (distance < best)
                best = distance;
        }

        return best;
    }

    // These are the raw embedding blobs held in the attachments, not HNSW's on-disk vectors, so the version-dependent
    // kernel selection in Hnsw.GetDistanceKernel (NormalizedTensor layout) does not apply to them.
    private static float Distance(Hnsw.SimilarityMethod similarityMethod, ReadOnlySpan<byte> query, ReadOnlySpan<byte> chunk)
    {
        if (query.Length != chunk.Length)
            return float.PositiveInfinity; // mismatched dimensions/quantization; treat as no match

        return similarityMethod switch
        {
            Hnsw.SimilarityMethod.CosineSimilaritySingles => Hnsw.CosineDistanceSingles(query, chunk),
            Hnsw.SimilarityMethod.CosineSimilarityI8 => Hnsw.CosineDistanceI8(query, chunk),
            Hnsw.SimilarityMethod.HammingDistance => Hnsw.HammingDistance(query, chunk),
            _ => float.PositiveInfinity
        };
    }
}

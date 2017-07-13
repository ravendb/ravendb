using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    internal static class BlittableJsonTextWriterExtensions
    {
        public static void WriteFacetQuery(this BlittableJsonTextWriter writer, DocumentConventions conventions, JsonOperationContext context, FacetQuery query)
        {
            writer.WriteObject(EntityToBlittable.ConvertEntityToBlittable(query, conventions, context));
        }

        public static void WriteMoreLikeThisQuery(this BlittableJsonTextWriter writer, DocumentConventions conventions, JsonOperationContext context, MoreLikeThisQuery query)
        {
            writer.WriteObject(EntityToBlittable.ConvertEntityToBlittable(query, conventions, context));
        }

        public static void WriteIndexQuery(this BlittableJsonTextWriter writer, DocumentConventions conventions, JsonOperationContext context, IndexQuery query)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(query.Query));
            writer.WriteString(query.Query);
            writer.WriteComma();

            if (query.PageSizeSet && query.PageSize >= 0)
            {
                writer.WritePropertyName(nameof(query.PageSize));
                writer.WriteInteger(query.PageSize);
                writer.WriteComma();
            }

            if (query.WaitForNonStaleResults)
            {
                writer.WritePropertyName(nameof(query.WaitForNonStaleResults));
                writer.WriteBool(query.WaitForNonStaleResults);
                writer.WriteComma();
            }

            if (query.WaitForNonStaleResultsAsOfNow)
            {
                writer.WritePropertyName(nameof(query.WaitForNonStaleResultsAsOfNow));
                writer.WriteBool(query.WaitForNonStaleResultsAsOfNow);
                writer.WriteComma();
            }

            if (query.CutoffEtag.HasValue)
            {
                writer.WritePropertyName(nameof(query.CutoffEtag));
                writer.WriteInteger(query.CutoffEtag.Value);
                writer.WriteComma();
            }

            if (query.Start > 0)
            {
                writer.WritePropertyName(nameof(query.Start));
                writer.WriteInteger(query.Start);
                writer.WriteComma();
            }

            if (query.WaitForNonStaleResultsTimeout.HasValue)
            {
                writer.WritePropertyName(nameof(query.WaitForNonStaleResultsTimeout));
                writer.WriteString(query.WaitForNonStaleResultsTimeout.Value.ToInvariantString());
                writer.WriteComma();
            }

            if (query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer)
            {
                writer.WritePropertyName(nameof(query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer));
                writer.WriteBool(query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer);
                writer.WriteComma();
            }

            if (query.DisableCaching)
            {
                writer.WritePropertyName(nameof(query.DisableCaching));
                writer.WriteBool(query.DisableCaching);
                writer.WriteComma();
            }

            if (query.ExplainScores)
            {
                writer.WritePropertyName(nameof(query.DisableCaching));
                writer.WriteBool(query.DisableCaching);
                writer.WriteComma();
            }

            if (query.ShowTimings)
            {
                writer.WritePropertyName(nameof(query.ShowTimings));
                writer.WriteBool(query.ShowTimings);
                writer.WriteComma();
            }

            if (query.SkipDuplicateChecking)
            {
                writer.WritePropertyName(nameof(query.SkipDuplicateChecking));
                writer.WriteBool(query.SkipDuplicateChecking);
                writer.WriteComma();
            }

            if (query.Includes != null && query.Includes.Length > 0)
            {
                writer.WriteArray(nameof(query.Includes), query.Includes);
                writer.WriteComma();
            }

            if (string.IsNullOrWhiteSpace(query.Transformer) == false)
            {
                writer.WritePropertyName(nameof(query.Transformer));
                writer.WriteString(query.Transformer);
                writer.WriteComma();

                if (query.TransformerParameters != null)
                {
                    writer.WritePropertyName(nameof(query.TransformerParameters));
                    writer.WriteObject(EntityToBlittable.ConvertEntityToBlittable(query.TransformerParameters, conventions, context));
                    writer.WriteComma();
                }
            }

            writer.WritePropertyName(nameof(query.QueryParameters));
            if (query.QueryParameters != null)
                writer.WriteObject(EntityToBlittable.ConvertEntityToBlittable(query.QueryParameters, conventions, context));
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }
    }
}
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Suggestion;
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

        public static void WriteSuggestionQuery(this BlittableJsonTextWriter writer, DocumentConventions conventions, JsonOperationContext context, SuggestionQuery query)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(query.IndexName));
            writer.WriteString(query.IndexName);
            writer.WriteComma();

            if (query.Popularity)
            {
                writer.WritePropertyName(nameof(query.Popularity));
                writer.WriteBool(query.Popularity);
                writer.WriteComma();
            }

            if (query.Accuracy.HasValue)
            {
                writer.WritePropertyName(nameof(query.Accuracy));
                writer.WriteDouble(query.Accuracy.Value);
                writer.WriteComma();
            }

            if (query.Distance.HasValue)
            {
                writer.WritePropertyName(nameof(query.Distance));
                writer.WriteString(query.Distance.Value.ToString());
                writer.WriteComma();
            }

            if (string.IsNullOrEmpty(query.Field) == false)
            {
                writer.WritePropertyName(nameof(query.Field));
                writer.WriteString(query.Field);
                writer.WriteComma();
            }

            if (string.IsNullOrEmpty(query.Term) == false)
            {
                writer.WritePropertyName(nameof(query.Term));
                writer.WriteString(query.Term);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(query.MaxSuggestions));
            writer.WriteInteger(query.MaxSuggestions);

            writer.WriteEndObject();
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

            writer.WritePropertyName(nameof(query.QueryParameters));
            if (query.QueryParameters != null)
                writer.WriteObject(EntityToBlittable.ConvertEntityToBlittable(query.QueryParameters, conventions, context));
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }
    }
}

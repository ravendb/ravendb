using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    internal static class BlittableJsonTextWriterExtensions
    {
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

#if FEATURE_EXPLAIN_SCORES
            if (query.ExplainScores)
            {
                writer.WritePropertyName(nameof(query.ExplainScores));
                writer.WriteBool(query.ExplainScores);
                writer.WriteComma();
            }
#endif

#if FEATURE_SHOW_TIMINGS
            if (query.ShowTimings)
            {
                writer.WritePropertyName(nameof(query.ShowTimings));
                writer.WriteBool(query.ShowTimings);
                writer.WriteComma();
            }
#endif

            if (query.SkipDuplicateChecking)
            {
                writer.WritePropertyName(nameof(query.SkipDuplicateChecking));
                writer.WriteBool(query.SkipDuplicateChecking);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(query.QueryParameters));
            if (query.QueryParameters != null)
                writer.WriteObject(EntityToBlittable.ConvertEntityToBlittable(query.QueryParameters, conventions, context, conventions.CreateSerializer(), documentInfo: null));
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }
    }
}

using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    internal static class BlittableJsonTextWriterExtensions
    {
        public static void WriteIndexQuery<T>(this AbstractBlittableJsonTextWriter writer, DocumentConventions conventions, JsonOperationContext context, IndexQuery<T> query)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(query.Query));
            writer.WriteString(query.Query);
            writer.WriteComma();

            if (query.WaitForNonStaleResults)
            {
                writer.WritePropertyName(nameof(query.WaitForNonStaleResults));
                writer.WriteBool(query.WaitForNonStaleResults);
                writer.WriteComma();
            }

            if (query.WaitForNonStaleResultsTimeout.HasValue)
            {
                writer.WritePropertyName(nameof(query.WaitForNonStaleResultsTimeout));
                writer.WriteString(query.WaitForNonStaleResultsTimeout.Value.ToInvariantString());
                writer.WriteComma();
            }

            if (query.SkipDuplicateChecking)
            {
                writer.WritePropertyName(nameof(query.SkipDuplicateChecking));
                writer.WriteBool(query.SkipDuplicateChecking);
                writer.WriteComma();
            }

            if (query.SkipStatistics)
            {
                writer.WritePropertyName(nameof(query.SkipStatistics));
                writer.WriteBool(query.SkipStatistics);
                writer.WriteComma();
            }

            writer.WritePropertyName(nameof(query.QueryParameters));
            if (query.QueryParameters != null)
                writer.WriteObject(conventions.Serialization.DefaultConverter.ToBlittable(query.QueryParameters, context));
            else
                writer.WriteNull();

            if (query.ProjectionBehavior.HasValue && query.ProjectionBehavior.Value != ProjectionBehavior.Default)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(query.ProjectionBehavior));
                writer.WriteString(query.ProjectionBehavior.ToString());
            }

            writer.WriteEndObject();
        }
    }
}

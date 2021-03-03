using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    internal static class BlittableJsonTextWriterExtensions
    {
        public static void WriteIndexQuery(this AbstractBlittableJsonTextWriter writer, DocumentConventions conventions, JsonOperationContext context, IndexQuery query)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(query.Query));
            writer.WriteString(query.Query);
            writer.WriteComma();

#pragma warning disable 618
            if (query.PageSizeSet && query.PageSize >= 0)
            {
                writer.WritePropertyName(nameof(query.PageSize));
                writer.WriteInteger(query.PageSize);
                writer.WriteComma();
            }
#pragma warning restore 618

            if (query.WaitForNonStaleResults)
            {
                writer.WritePropertyName(nameof(query.WaitForNonStaleResults));
                writer.WriteBool(query.WaitForNonStaleResults);
                writer.WriteComma();
            }

#pragma warning disable 618
            if (query.Start > 0)
            {
                writer.WritePropertyName(nameof(query.Start));
                writer.WriteInteger(query.Start);
                writer.WriteComma();
            }
#pragma warning restore 618

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

            if (query.SkipDuplicateChecking)
            {
                writer.WritePropertyName(nameof(query.SkipDuplicateChecking));
                writer.WriteBool(query.SkipDuplicateChecking);
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

using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class FieldsToFetchToken : QueryToken
    {
        public readonly string[] FieldsToFetch;
        public readonly string[] Projections;
        public readonly bool IsCustomFunction;
        public readonly string SourceAlias;

        private FieldsToFetchToken(string[] fieldsToFetch, string[] projections, bool isCustomFunction, string sourceAlias)
        {
            FieldsToFetch = fieldsToFetch;
            Projections = projections;
            IsCustomFunction = isCustomFunction;
            SourceAlias = sourceAlias;
        }

        public static FieldsToFetchToken Create(string[] fieldsToFetch, string[] projections, bool isCustomFunction, string sourceAlias = null)
        {
            if (fieldsToFetch == null || fieldsToFetch.Length == 0)
                throw new ArgumentNullException(nameof(fieldsToFetch));

            if (isCustomFunction == false && projections != null && projections.Length != fieldsToFetch.Length)
                throw new ArgumentNullException(nameof(projections), "Length of projections must be the same as length of fields to fetch.");

            return new FieldsToFetchToken(fieldsToFetch, projections, isCustomFunction, sourceAlias);
        }

        public override void WriteTo(StringBuilder writer)
        {
            for (var i = 0; i < FieldsToFetch.Length; i++)
            {
                var fieldToFetch = FieldsToFetch[i];

                if (i > 0)
                    writer.Append(", ");

                if (fieldToFetch == null)
                {
                    writer.Append("null");
                }
                else
                {
                    WriteField(writer, fieldToFetch);
                }

                if (IsCustomFunction)
                    continue;
                
                var projection = Projections?[i];

                if (projection == null || string.Equals(fieldToFetch, projection))
                    continue;

                writer.Append(" as ");
                writer.Append(projection);
            }
        }
    }
}

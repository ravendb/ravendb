using System;
using System.Collections.Generic;

using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Transformers;
using Sparrow;

namespace Raven.Server.Documents.Queries
{
    public class FieldsToFetch
    {
        public readonly Dictionary<string, FieldToFetch> Fields;

        public readonly bool ExtractAllFromIndexAndDocument;

        public readonly bool AnyExtractableFromIndex;

        public readonly bool IsProjection;

        public readonly bool IsDistinct;

        public readonly bool IsTransformation;

        public FieldsToFetch(IndexQueryServerSide query, IndexDefinitionBase indexDefinition, Transformer transformer)
            : this(query.FieldsToFetch, indexDefinition, transformer)
        {
            IsDistinct = query.IsDistinct && IsProjection;
        }

        public FieldsToFetch(string[] fieldsToFetch, IndexDefinitionBase indexDefinition, Transformer transformer)
        {
            Fields = GetFieldsToFetch(fieldsToFetch, indexDefinition, out AnyExtractableFromIndex);
            IsProjection = Fields != null && Fields.Count > 0;
            IsDistinct = false;

            if (transformer != null)
            {
                AnyExtractableFromIndex = true;
                ExtractAllFromIndexAndDocument = Fields == null || Fields.Count == 0; // extracting all from index only if fields are not specified
                IsTransformation = true;
            }
        }

        private static Dictionary<string, FieldToFetch> GetFieldsToFetch(string[] fieldsToFetch, IndexDefinitionBase indexDefinition, out bool anyExtractableFromIndex)
        {
            anyExtractableFromIndex = false;

            if (fieldsToFetch == null || fieldsToFetch.Length == 0)
                return null;

            var result = new Dictionary<string, FieldToFetch>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < fieldsToFetch.Length; i++)
            {
                var fieldToFetch = fieldsToFetch[i];

                IndexField value;
                var extract = indexDefinition != null && indexDefinition.TryGetField(fieldToFetch, out value) && value.Storage == FieldStorage.Yes;
                if (extract)
                    anyExtractableFromIndex = true;

                result[fieldToFetch] = new FieldToFetch(fieldToFetch, extract | indexDefinition.HasDynamicFields);
            }

            anyExtractableFromIndex |= indexDefinition.HasDynamicFields;

            return result;
        }

        public bool ContainsField(string name)
        {
            return Fields == null || Fields.ContainsKey(name);
        }

        public class FieldToFetch
        {
            public FieldToFetch(string name, bool canExtractFromIndex)
            {
                Name = name;
                CanExtractFromIndex = canExtractFromIndex;
            }

            public readonly StringSegment Name;

            public readonly bool CanExtractFromIndex;
        }
    }
}
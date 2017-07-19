using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Transformers;
using Sparrow;

namespace Raven.Server.Documents.Queries
{
    public class FieldsToFetch
    {
        public readonly Dictionary<string, FieldToFetch> Fields;

        public readonly bool ExtractAllFromIndex;

        public readonly bool ExtractAllFromDocument;

        public readonly bool AnyExtractableFromIndex;

        public readonly bool IsProjection;

        public readonly bool IsDistinct;

        public readonly bool IsTransformation;

        public FieldsToFetch(IndexQueryServerSide query, IndexDefinitionBase indexDefinition, Transformer transformer)
            : this(query.Metadata.SelectFields, indexDefinition, transformer)
        {
            IsDistinct = query.IsDistinct && IsProjection;
        }

        public FieldsToFetch(SelectField[] fieldsToFetch, IndexDefinitionBase indexDefinition, Transformer transformer)
        {
            Fields = GetFieldsToFetch(fieldsToFetch, indexDefinition, out AnyExtractableFromIndex, out bool extractAllStoredFields);
            IsProjection = Fields != null && Fields.Count > 0;
            IsDistinct = false;

            if (extractAllStoredFields)
            {
                AnyExtractableFromIndex = true;
                ExtractAllFromIndex = true; // we want to add dynamic fields also to the result (stored only)
                IsProjection = true;
            }

            if (transformer != null)
            {
                AnyExtractableFromIndex = true;
                ExtractAllFromIndex = ExtractAllFromDocument = Fields == null || Fields.Count == 0; // extracting all from index only if fields are not specified
                IsTransformation = true;
            }
        }

        private static Dictionary<string, FieldToFetch> GetFieldsToFetch(SelectField[] fieldsToFetch, IndexDefinitionBase indexDefinition, out bool anyExtractableFromIndex, out bool extractAllStoredFields)
        {
            anyExtractableFromIndex = false;
            extractAllStoredFields = false;

            if (fieldsToFetch == null || fieldsToFetch.Length == 0)
                return null;

            var result = new Dictionary<string, FieldToFetch>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < fieldsToFetch.Length; i++)
            {
                var fieldToFetch = fieldsToFetch[i];

                if (string.IsNullOrWhiteSpace(fieldToFetch.Name))
                    continue;

                if (indexDefinition == null)
                {
                    result[fieldToFetch.Name] = new FieldToFetch(fieldToFetch.Name, false);
                    continue;
                }

                if (fieldToFetch.Name[0] == '_' && fieldToFetch.Name == Constants.Documents.Indexing.Fields.AllStoredFields)
                {
                    if (result.Count > 0)
                        result.Clear(); // __all_stored_fields should only return stored fields so we are ensuring that no other fields will be returned

                    extractAllStoredFields = true;

                    foreach (var kvp in indexDefinition.MapFields)
                    {
                        var stored = kvp.Value.Storage == FieldStorage.Yes;
                        if (stored == false)
                            continue;

                        anyExtractableFromIndex = true;
                        result[kvp.Key] = new FieldToFetch(kvp.Key, canExtractFromIndex: true);
                    }

                    return result;
                }

                IndexField value;
                var extract = indexDefinition.TryGetField(fieldToFetch.Name, out value) && value.Storage == FieldStorage.Yes;
                if (extract)
                    anyExtractableFromIndex = true;

                result[fieldToFetch.Name] = new FieldToFetch(fieldToFetch.Name, extract | indexDefinition.HasDynamicFields);
            }

            if (indexDefinition != null)
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
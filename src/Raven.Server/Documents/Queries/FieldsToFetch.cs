using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Sparrow;

namespace Raven.Server.Documents.Queries
{
    public class FieldsToFetch
    {
        public readonly Dictionary<string, FieldToFetch> Fields;

        public readonly bool ExtractAllFromIndex;

        public readonly bool AnyExtractableFromIndex;

        public readonly bool SingleFieldNoAlias;

        public readonly bool IsProjection;

        public readonly bool IsDistinct;


        public FieldsToFetch(IndexQueryServerSide query, IndexDefinitionBase indexDefinition)
            : this(query.Metadata.SelectFields, indexDefinition)
        {
            IsDistinct = query.Metadata.IsDistinct && IsProjection;
        }

        public FieldsToFetch(SelectField[] fieldsToFetch, IndexDefinitionBase indexDefinition)
        {
            Fields = GetFieldsToFetch(fieldsToFetch, indexDefinition, out AnyExtractableFromIndex, out bool extractAllStoredFields, out SingleFieldNoAlias);
            IsProjection = Fields != null && Fields.Count > 0;
            IsDistinct = false;

            if (extractAllStoredFields)
            {
                AnyExtractableFromIndex = true;
                ExtractAllFromIndex = true; // we want to add dynamic fields also to the result (stored only)
                IsProjection = true;
            }
        }

        private static FieldToFetch GetFieldToFetch(
            IndexDefinitionBase indexDefinition, 
            SelectField selectField,
            Dictionary<string, FieldToFetch> results,
            out string selectFieldKey,
            ref bool anyExtractableFromIndex, 
            ref bool extractAllStoredFields)
        {
            selectFieldKey = selectField.Alias ?? selectField.Name;
            var selectFieldName = selectField.Name;
            if (selectField.ValueTokenType != null)
            {
                return new FieldToFetch(string.Empty, selectField, selectField.Alias,
                    canExtractFromIndex: false, isDocumentId: false);
            }
            if (selectField.Function != null)
            {

                var fieldToFetch = new FieldToFetch(string.Empty, selectField, selectField.Alias,
                    canExtractFromIndex: false, isDocumentId: false);
                fieldToFetch.FunctionArgs = new FieldToFetch[selectField.FunctionArgs.Length];
                for (int j = 0; j < selectField.FunctionArgs.Length; j++)
                {
                    bool ignored = false;
                    fieldToFetch.FunctionArgs[j] = GetFieldToFetch(indexDefinition,
                        selectField.FunctionArgs[j],
                        null,
                        out _,
                        ref ignored,
                        ref ignored
                    );
                }
                return fieldToFetch;
            }

            if (selectFieldName == null)
            {
                if (selectField.IsGroupByKey == false)
                    return null;

                if (selectField.GroupByKeys.Length == 1)
                {
                    selectFieldName = selectField.GroupByKeys[0];

                    if (selectFieldKey == null)
                        selectFieldKey = selectFieldName;
                }
                else
                {
                    selectFieldKey = selectFieldKey ?? "Key";
                    return new FieldToFetch(selectFieldKey, selectField.GroupByKeys);
                }
            }
            if (indexDefinition == null)
            {
                return new FieldToFetch(selectFieldName, selectField, selectField.Alias, canExtractFromIndex: false, isDocumentId: false);
            }
            if (selectFieldName[0] == '_')
            {
                if (selectFieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                {
                    anyExtractableFromIndex = true;
                    return new FieldToFetch(selectFieldName, selectField, selectField.Alias, canExtractFromIndex: false, isDocumentId: true);
                }

                if (selectFieldName == Constants.Documents.Indexing.Fields.AllStoredFields)
                {
                    if (results == null)
                        ThrowInvalidFetchAllStoredDocuments();
                    Debug.Assert(results != null);
                    results.Clear(); // __all_stored_fields should only return stored fields so we are ensuring that no other fields will be returned

                    extractAllStoredFields = true;

                    foreach (var kvp in indexDefinition.MapFields)
                    {
                        var stored = kvp.Value.Storage == FieldStorage.Yes;
                        if (stored == false)
                            continue;

                        anyExtractableFromIndex = true;
                        results[kvp.Key] = new FieldToFetch(kvp.Key, null, null, canExtractFromIndex: true, isDocumentId: false);
                    }

                    return null;
                }
            }

            var extract = indexDefinition.TryGetField(selectFieldName, out IndexField value) && value.Storage == FieldStorage.Yes;
            if (extract)
                anyExtractableFromIndex = true;

            return new FieldToFetch(selectFieldName, selectField, selectField.Alias, extract | indexDefinition.HasDynamicFields, isDocumentId: false);
        }

        private static void ThrowInvalidFetchAllStoredDocuments()
        {
            throw new InvalidOperationException("Cannot fetch all stored path from a nested method");
        }

        private static Dictionary<string, FieldToFetch> GetFieldsToFetch(
            SelectField[] selectFields, 
            IndexDefinitionBase indexDefinition, 
            out bool anyExtractableFromIndex, 
            out bool extractAllStoredFields,
            out bool singleFieldNoAlias)
        {
            anyExtractableFromIndex = false;
            extractAllStoredFields = false;
            singleFieldNoAlias = false;

            if (selectFields == null || selectFields.Length == 0)
                return null;

            var result = new Dictionary<string, FieldToFetch>(StringComparer.OrdinalIgnoreCase);
            singleFieldNoAlias = selectFields.Length == 1 && selectFields[0].Alias == null;
            for (var i = 0; i < selectFields.Length; i++)
            {
                var selectField = selectFields[i];
                string key;
                var val = GetFieldToFetch(indexDefinition, selectField, result, 
                    out key, ref anyExtractableFromIndex, ref extractAllStoredFields);
                if (extractAllStoredFields)
                    return result;
                if (val == null)
                    continue;
                result[key] = val;
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
            public FieldToFetch(string name, SelectField queryField, string projectedName, bool canExtractFromIndex, bool isDocumentId)
            {
                Name = name;
                QueryField = queryField;
                ProjectedName = projectedName;
                IsDocumentId = isDocumentId;
                CanExtractFromIndex = canExtractFromIndex;
            }

            public FieldToFetch(string projectedName, string[] components)
            {
                ProjectedName = projectedName;
                Components = components;
                IsCompositeField = true;
                CanExtractFromIndex = false;
            }

            public readonly StringSegment Name;

            public readonly SelectField QueryField;

            public readonly string ProjectedName;

            public readonly bool CanExtractFromIndex;

            public readonly bool IsCompositeField;

            public readonly bool IsDocumentId;

            public readonly string[] Components;

            public FieldToFetch[] FunctionArgs;
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Sparrow.Json;

namespace Raven.Client.Documents
{
    public class BlittableOperation
    {
        public BlittableOperation()
        {
        }

        public bool EntityChanged(BlittableJsonReaderObject newObj, InMemoryDocumentSessionOperations.DocumentInfo documentInfo, IDictionary<string, DocumentsChanges[]> changes)
        {
            // prevent saves of a modified read only entity
            object readOnly;
            documentInfo.Metadata.TryGet(Constants.Headers.RavenReadOnly, out readOnly);
            BlittableJsonReaderObject newMetadata;
            var newReadOnly = "false";
            if (newObj.TryGet(Constants.Metadata.Key, out newMetadata))
                newMetadata.TryGet(Constants.Headers.RavenReadOnly, out newReadOnly);

            if ((readOnly != null) && (readOnly.Equals("true")) && (newReadOnly.Equals("true")))
                return false;

            var docChanges = new List<DocumentsChanges>() { };

            if (!documentInfo.IsNewDocument && documentInfo.Document != null)
                return CompareBlittable(documentInfo.Id, documentInfo.Document, newObj, changes, docChanges);

            if (changes == null)
                return true;

            NewChange(null, null, null, docChanges, DocumentsChanges.ChangeType.DocumentAdded);
            changes[documentInfo.Id] = docChanges.ToArray();
            return true;
        }

        private static bool CompareBlittable(string id, BlittableJsonReaderObject originalBlittable,
            BlittableJsonReaderObject newBlittable, IDictionary<string, DocumentsChanges[]> changes,
            List<DocumentsChanges> docChanges)
        {
            var newBlittableProps = newBlittable.GetPropertyNames();
            var oldBlittableProps = originalBlittable.GetPropertyNames();
            var newFields = newBlittableProps.Except(oldBlittableProps);
            var removedFields = oldBlittableProps.Except(newBlittableProps);

            var propertiesIds = newBlittable.GetPropertiesByInsertionOrder();

            foreach (var field in removedFields)
            {
                if (changes == null)
                    return true;
                NewChange(field, null, null, docChanges, DocumentsChanges.ChangeType.RemovedField);
            }

            foreach (var propId in propertiesIds)
            {
                var newPropInfo = newBlittable.GetPropertyByIndex(propId);

                if (newPropInfo.Item1.Equals(Constants.Headers.RavenLastModified))
                    continue;

                if (newFields.Contains(newPropInfo.Item1))
                {
                    if (changes == null)
                        return true;
                    NewChange(newPropInfo.Item1, newPropInfo.Item2, null, docChanges, DocumentsChanges.ChangeType.NewField);
                    continue;
                }


                var oldPropId = originalBlittable.GetPropertyIndex(newPropInfo.Item1);
                var oldPropInfo = originalBlittable.GetPropertyByIndex(oldPropId);

                switch ((newPropInfo.Item3 & BlittableJsonReaderBase.TypesMask))
                {
                    case BlittableJsonToken.Integer:
                    case BlittableJsonToken.Boolean:
                    case BlittableJsonToken.Float:
                    case BlittableJsonToken.CompressedString:
                    case BlittableJsonToken.String:
                        if (newPropInfo.Item2.Equals(oldPropInfo.Item2))
                            break;

                        if (changes == null)
                            return true;
                        NewChange(newPropInfo.Item1, newPropInfo.Item2, oldPropInfo.Item2, docChanges,
                            DocumentsChanges.ChangeType.FieldChanged);
                        break;
                    case BlittableJsonToken.Null:
                        break;
                    case BlittableJsonToken.StartArray:
                        var newArray = newPropInfo.Item2 as BlittableJsonReaderArray;
                        var oldArray = oldPropInfo.Item2 as BlittableJsonReaderArray;

                        if ((newArray == null) || (oldArray == null))
                            throw new InvalidDataException("Invalid blittable");

                        if (!(newArray.Except(oldArray).Any()))
                            break;

                        if (changes == null)
                            return true;
                        NewChange(newPropInfo.Item1, newPropInfo.Item2, oldPropInfo.Item2, docChanges,
                            DocumentsChanges.ChangeType.FieldChanged);
                        break;
                    case BlittableJsonToken.StartObject:
                    {
                        var changed = CompareBlittable(id, oldPropInfo.Item2 as BlittableJsonReaderObject,
                            newPropInfo.Item2 as BlittableJsonReaderObject, changes, docChanges);
                        if (changes == null)
                            return changed;
                        break;
                    }
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if ((changes == null) || (docChanges.Count <= 0)) return false;

            changes[id] = docChanges.ToArray();
            return true;
        }

        private static void NewChange(string name, object newValue, object oldValue, List<DocumentsChanges> docChanges, DocumentsChanges.ChangeType change)
        {
            docChanges.Add(new DocumentsChanges()
            {
                FieldName = name,
                FieldNewValue = newValue,
                FieldOldValue = oldValue,
                Change = change
            });
        }
    }
}
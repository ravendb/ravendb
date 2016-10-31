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

            var newProp = new BlittableJsonReaderObject.PropertyDetails();
            var oldProp = new BlittableJsonReaderObject.PropertyDetails();

            foreach (var propId in propertiesIds)
            {
                newBlittable.GetPropertyByIndex(propId, ref newProp);

                if (newProp.Name.Equals(Constants.Headers.RavenLastModified))
                    continue;

                if (newFields.Contains(newProp.Name))
                {
                    if (changes == null)
                        return true;
                    NewChange(newProp.Name, newProp.Value, null, docChanges, DocumentsChanges.ChangeType.NewField);
                    continue;
                }


                var oldPropId = originalBlittable.GetPropertyIndex(newProp.Name);
                originalBlittable.GetPropertyByIndex(oldPropId, ref oldProp);

                switch ((newProp.Token & TypesMask))
                {
                    case BlittableJsonToken.Integer:
                    case BlittableJsonToken.Boolean:
                    case BlittableJsonToken.Float:
                    case BlittableJsonToken.CompressedString:
                    case BlittableJsonToken.String:
                        if (newProp.Value.Equals(oldProp.Value))
                            break;

                        if (changes == null)
                            return true;
                        NewChange(newProp.Name, newProp.Value, oldProp.Value, docChanges,
                            DocumentsChanges.ChangeType.FieldChanged);
                        break;
                    case BlittableJsonToken.Null:
                        break;
                    case BlittableJsonToken.StartArray:
                        var newArray = newProp.Value as BlittableJsonReaderArray;
                        var oldArray = oldProp.Value as BlittableJsonReaderArray;

                        if ((newArray == null) || (oldArray == null))
                            throw new InvalidDataException("Invalid blittable");

                        if (!(newArray.Except(oldArray).Any()))
                            break;

                        if (changes == null)
                            return true;
                        NewChange(newProp.Name, newProp.Value, oldProp.Value, docChanges,
                            DocumentsChanges.ChangeType.FieldChanged);
                        break;
                    case BlittableJsonToken.StartObject:
                    {
                        var changed = CompareBlittable(id, oldProp.Value as BlittableJsonReaderObject,
                            newProp.Value as BlittableJsonReaderObject, changes, docChanges);
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
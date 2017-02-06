using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Document;
using Sparrow.Json;

namespace Raven.NewClient.Client.Blittable
{
    public class BlittableOperation
    {
        public BlittableOperation()
        {
        }

        public bool EntityChanged(BlittableJsonReaderObject newObj, DocumentInfo documentInfo, IDictionary<string, DocumentsChanges[]> changes)
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

                if (newProp.Name.Equals(Constants.Metadata.LastModified))
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

                switch ((newProp.Token & BlittableJsonReaderBase.TypesMask))
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

                        var changed = CompareBlittableArray(newArray, oldArray);
                        if (!(changed))
                            break;

                        if (changes == null)
                            return true;
                        NewChange(newProp.Name, newProp.Value, oldProp.Value, docChanges,
                            DocumentsChanges.ChangeType.FieldChanged);
                        break;
                    case BlittableJsonToken.StartObject:
                    {
                        changed = CompareBlittable(id, oldProp.Value as BlittableJsonReaderObject,
                            newProp.Value as BlittableJsonReaderObject, changes, docChanges);
                        if ( (changes == null) && (changed))
                            return true;
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

        private static bool CompareBlittableArray(BlittableJsonReaderArray newArray, BlittableJsonReaderArray oldArray)
        {
            if (newArray.Length != oldArray.Length)
                return true;
            var type = newArray.GetArrayType();
            switch (type)
            {
                case BlittableJsonToken.StartObject:
                    foreach (var item in newArray.Items)
                    {
                        return oldArray.Items.Select(oldItem => 
                        CompareBlittable("", (BlittableJsonReaderObject) item, (BlittableJsonReaderObject) oldItem, null, null))
                        .All(change => change);
                    }
                    break;
                case BlittableJsonToken.StartArray:
                    foreach (var item in newArray.Items)
                    {
                        return oldArray.Items.Select(oldItem => 
                        CompareBlittableArray((BlittableJsonReaderArray) item, (BlittableJsonReaderArray) oldItem))
                        .All(change => change);
                    }
                    break;
                case BlittableJsonToken.Integer:
                case BlittableJsonToken.Float:
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                case BlittableJsonToken.Boolean:
                    return (!(!(newArray.Except(oldArray).Any()) && newArray.Length == oldArray.Length));
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return false;

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
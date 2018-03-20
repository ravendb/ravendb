using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration
{
    public abstract class GenericDatabaseMigrator<TConnection> : IDatabaseDriver where TConnection : IDisposable
    {
        public abstract DatabaseSchema FindSchema();

        public async Task Migrate(MigrationSettings settings, DatabaseSchema dbSchema, DocumentDatabase db, DocumentsOperationContext context)
        {
            using (var enumerationConnection = OpenConnection())
            using (var referencesConnection = OpenConnection())
                //TODO: operation + progress, etc. 
            using (var writer = new SqlMigrationWriter(context, settings.BatchSize))
            {
                foreach (var collectionToImport in settings.Collections)
                {
                    var tableSchema = GetSchemaForTable(collectionToImport.SourceTableName, dbSchema);
                    var specialColumns = FindSpecialColumns(collectionToImport.SourceTableName, dbSchema);

                    using (var patcher = new JsPatcher(collectionToImport, context))
                    {
                        var references = ResolveReferences(collectionToImport, dbSchema, settings.Collections);

                        InitializeDataProviders(references, referencesConnection);

                        try
                        {
                            foreach (var doc in EnumerateTable(GetQueryForCollection(collectionToImport), specialColumns, enumerationConnection))
                            {
                                doc.TableName = collectionToImport.SourceTableName;
                                doc.SetCollection(collectionToImport.Name);

                                var id = GenerateDocumentId(doc.Collection, GetColumns(doc.SpecialColumnsValues, tableSchema.PrimaryKeyColumns));

                                FillDocumentFields(doc.Object, doc.SpecialColumnsValues, references);

                                var docBlittable = patcher.Patch(doc.ToBllitable(context));
                                //TODO: support for throw skip

                                await writer.InsertDocument(docBlittable, id, null); //TODO: pass attachments
                            }
                        }
                        finally
                        {
                            DisposeDataProviders(references);
                        }
                    }
                }
            }
        }

        private void FillDocumentFields(DynamicJsonValue value, DynamicJsonValue specialColumns, List<ReferenceInformation> references)
        {
            foreach (var refInfo in references)
            {
                switch (refInfo.Type)
                {
                    case ReferenceType.ArrayEmbed:
                        var arrayWithEmbeddedObjects = (EmbeddedArrayValue)refInfo.DataProvider.Provide(specialColumns);
                        value[refInfo.PropertyName] = arrayWithEmbeddedObjects.ArrayOfNestedObjects;

                        if (refInfo.ChildReferences != null)
                        {
                            var idx = 0;
                            foreach (DynamicJsonValue arrayItem in arrayWithEmbeddedObjects.ArrayOfNestedObjects)
                            {
                                FillDocumentFields(arrayItem, arrayWithEmbeddedObjects.SpecialColumnsValues[idx], refInfo.ChildReferences);
                                idx++;
                            }
                        }

                        break;
                    case ReferenceType.ObjectEmbed:
                        var embeddedObjectValue = (EmbeddedObjectValue)refInfo.DataProvider.Provide(specialColumns);
                        value[refInfo.PropertyName] = embeddedObjectValue?.Object; // fill in value or null
                        
                        if (embeddedObjectValue != null)
                        {
                            if (refInfo.ChildReferences != null)
                            {
                                FillDocumentFields(embeddedObjectValue.Object, embeddedObjectValue.SpecialColumnsValues, refInfo.ChildReferences);
                            }
                        }
                        break;
                    case ReferenceType.ArrayLink:
                        var arrayWithLinks = (DynamicJsonArray)refInfo.DataProvider.Provide(specialColumns);
                        value[refInfo.PropertyName] = arrayWithLinks;
                        break;
                    case ReferenceType.ObjectLink:
                        var linkValue = (string)refInfo.DataProvider.Provide(specialColumns);
                        value[refInfo.PropertyName] = linkValue;
                        break;
                }
            }
        }

        private void DisposeDataProviders(List<ReferenceInformation> references)
        {
            foreach (var referenceInformation in references)
            {
                if (referenceInformation.ChildReferences != null)
                {
                    DisposeDataProviders(referenceInformation.ChildReferences);
                }

                referenceInformation.DataProvider.Dispose();
            }
        }

        private void InitializeDataProviders(List<ReferenceInformation> references, TConnection connection)
        {
            references.ForEach(reference =>
            {
                switch (reference.Type)
                {
                    case ReferenceType.ArrayEmbed:
                        reference.DataProvider = CreateArrayEmbedDataProvider(reference, connection);
                        break;
                    case ReferenceType.ArrayLink:
                        reference.DataProvider = CreateArrayLinkDataProvider(reference, connection);
                        break;
                    case ReferenceType.ObjectEmbed:
                        reference.DataProvider = CreateObjectEmbedDataProvider(reference, connection);
                        break;
                    case ReferenceType.ObjectLink:
                        reference.DataProvider = CreateObjectLinkDataProvider(reference);
                        break;
                }

                if (reference.ChildReferences != null)
                {
                    InitializeDataProviders(reference.ChildReferences, connection);
                }
            });
        }

        private List<ReferenceInformation> ResolveReferences(CollectionWithReferences sourceCollection, DatabaseSchema dbSchema, List<RootCollection> allCollections)
        {
            var result = new List<ReferenceInformation>();

            if (sourceCollection.NestedCollections != null)
            {
                foreach (var embeddedCollection in sourceCollection.NestedCollections)
                {
                    var reference = CreateReference(embeddedCollection, dbSchema, sourceCollection, allCollections);
                    var resolvedReferences = ResolveReferences(embeddedCollection, dbSchema, allCollections);
                    reference.ChildReferences = resolvedReferences.Count > 0 ? resolvedReferences : null;
                    result.Add(reference);
                }
            }

            if (sourceCollection.LinkedCollections != null)
            {
                foreach (var linkedCollection in sourceCollection.LinkedCollections)
                    result.Add(CreateReference(linkedCollection, dbSchema, sourceCollection, allCollections));
            }

            return result;
        }

        private ReferenceInformation CreateReference(AbstractCollection destinationCollection, DatabaseSchema dbSchema, CollectionWithReferences sourceCollection,
            List<RootCollection> allCollections)
        {
            var sourceSchema = GetSchemaForTable(sourceCollection.SourceTableName, dbSchema);
            var destinationSchema = GetSchemaForTable(destinationCollection.SourceTableName, dbSchema);

            var outgoingReference = sourceSchema.References.SingleOrDefault(x => x.Table == destinationCollection.SourceTableName);
            var incomingReference = destinationSchema.References.SingleOrDefault(x => x.Table == sourceCollection.SourceTableName);

            if (outgoingReference != null && incomingReference != null)
            {
                throw new NotSupportedException();
            }

            if (outgoingReference == null && incomingReference == null)
            {
                throw new InvalidOperationException("Unable to resolve reference: " + sourceCollection.SourceTableName + " -> " + destinationCollection.SourceTableName);
            }

            var specialColumns = FindSpecialColumns(destinationCollection.SourceTableName, dbSchema);
            var documentColumns = destinationSchema.Columns
                .Where(x => x.Type != ColumnType.Unsupported)
                .Select(x => x.Name)
                .Except(specialColumns)
                .ToHashSet();

            var referenceInformation = new ReferenceInformation
            {
                SourcePrimaryKeyColumns = sourceSchema.PrimaryKeyColumns,
                SourceTableName = destinationCollection.SourceTableName,
                TargetPrimaryKeyColumns = destinationSchema.PrimaryKeyColumns,
                PropertyName = destinationCollection.Name,
                ForeignKeyColumns = incomingReference != null ? incomingReference.Columns : outgoingReference.Columns,
                TargetDocumentColumns = documentColumns,
                TargetSpecialColumnsNames = specialColumns,
                Type = (destinationCollection is EmbeddedCollection)
                    ? (incomingReference != null ? ReferenceType.ObjectEmbed : ReferenceType.ArrayEmbed)
                    : (incomingReference != null ? ReferenceType.ObjectLink : ReferenceType.ArrayLink),
            };

            if (destinationCollection is LinkedCollection)
            {
                // linked connection
                var collectionToUseInLinks = allCollections.Single(x => x.SourceTableName == destinationCollection.SourceTableName)
                    .Name;

                referenceInformation.CollectionNameToUseInLinks = collectionToUseInLinks;
            }

            return referenceInformation;
        }

        protected HashSet<string> FindSpecialColumns(string tableName, DatabaseSchema dbSchema)
        {
            var mainSchema = GetSchemaForTable(tableName, dbSchema);

            var result = new HashSet<string>();
            mainSchema.PrimaryKeyColumns.ForEach(x => result.Add(x));

            foreach (var fkCandidate in dbSchema.Tables.Values)
            foreach (var tableReference in fkCandidate.References
                .Where(x => x.Table == tableName))
            {
                tableReference.Columns.ForEach(x => result.Add(x));
            }

            return result;
        }

        private TableSchema GetSchemaForTable(string name, DatabaseSchema dbSchema)
        {
            if (dbSchema.Tables.TryGetValue(name, out var tableSchema) == false)
            {
                throw new ArgumentException("Cannot find table " + name + " in database schema");
            }

            return tableSchema;
        }

        public static object[] GetColumns(DynamicJsonValue columns, List<string> columnsToUse)
        {
            var result = new object[columnsToUse.Count];
            for (var i = 0; i < columnsToUse.Count; i++)
            {
                result[i] = columns[columnsToUse[i]];
            }

            return result;
        }

        protected string GenerateDocumentId(string collection, object[] values)
        {
            foreach (var t in values)
            {
                if (t == null)
                    return null;
            }

            return collection + "/" + string.Join("/", values);
        }

        protected DynamicJsonValue ExtractFromReader(DbDataReader reader, IEnumerable<string> columnNames)
        {
            var document = new DynamicJsonValue();

            foreach (string column in columnNames)
            {
                document[column] = ExtractValue(reader[column]);
            }

            return document;
        }

        private object ExtractValue(object value)
        {
            switch (value)
            {
                case null:
                case DBNull _:
                    return null;
                case string str:
                    return str;
                case byte[] byteArray:
                    return System.Convert.ToBase64String(byteArray);
                case Guid guid:
                    return guid.ToString();
            }

            return value;
        }

        protected abstract string GetQueryForCollection(RootCollection collection);

        protected abstract IEnumerable<SqlMigrationDocument> EnumerateTable(string tableQuery, HashSet<string> specialColumns, TConnection connection);

        protected abstract IDataProvider<EmbeddedObjectValue> CreateObjectEmbedDataProvider(ReferenceInformation refInfo, TConnection connection);
        protected abstract IDataProvider<DynamicJsonArray> CreateArrayLinkDataProvider(ReferenceInformation refInfo, TConnection connection);
        protected abstract IDataProvider<EmbeddedArrayValue> CreateArrayEmbedDataProvider(ReferenceInformation refInfo, TConnection connection);

        protected abstract string QuoteTable(string tableName);
        protected abstract string QuoteColumn(string columnName);

        protected IDataProvider<string> CreateObjectLinkDataProvider(ReferenceInformation refInfo)
        {
            return new LocalDataProvider<string>(specialColumns =>
                GenerateDocumentId(refInfo.CollectionNameToUseInLinks, GetColumns(specialColumns, refInfo.ForeignKeyColumns)));
        }

        protected abstract TConnection OpenConnection();
    }
}

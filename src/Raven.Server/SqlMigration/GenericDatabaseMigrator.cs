using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.MsSQL;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using DbProviderFactories = Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters.DbProviderFactories;

namespace Raven.Server.SqlMigration
{
    public abstract class GenericDatabaseMigrator : IDatabaseDriver
    {
        protected readonly string ConnectionString;
        
        public abstract DatabaseSchema FindSchema();

        protected GenericDatabaseMigrator(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public (BlittableJsonReaderObject Document, string Id) Test(MigrationTestSettings settings, DatabaseSchema dbSchema, DocumentsOperationContext context)
        {
            using (var enumerationConnection = OpenConnection())
            using (var referencesConnection = OpenConnection())
            {
                var collectionToImport = settings.Collection;
                var tableSchema = dbSchema.GetTable(collectionToImport.SourceTableSchema, collectionToImport.SourceTableName);
                var specialColumns = dbSchema.FindSpecialColumns(collectionToImport.SourceTableSchema, collectionToImport.SourceTableName);

                string CollectionNameProvider(string schema, string name) => settings.CollectionsMapping.Single(x => x.TableSchema == schema && x.TableName == name).CollectionName;
                
                using (var patcher = new JsPatcher(collectionToImport, context))
                {
                    var references = ResolveReferences(collectionToImport, dbSchema, CollectionNameProvider);

                    InitializeDataProviders(references, referencesConnection);

                    try
                    {
                        Dictionary<string, object> queryParameters = null;
                        var queryToUse = settings.Mode == MigrationTestMode.First 
                            ? GetQueryForCollection(collectionToImport) // we limit rows count internally by passing rowsLimit: 1
                            : GetQueryByPrimaryKey(collectionToImport, tableSchema, settings.PrimaryKeyValues, out queryParameters);
                        
                        foreach (var doc in EnumerateTable(queryToUse, collectionToImport.ColumnsMapping, specialColumns,
                            collectionToImport.AttachmentNameMapping, enumerationConnection, rowsLimit: 1, queryParameters: queryParameters))
                        {
                            doc.SetCollection(collectionToImport.Name);
                            
                            var id = GenerateDocumentId(doc.Collection, GetColumns(doc.SpecialColumnsValues, tableSchema.PrimaryKeyColumns));

                            FillDocumentFields(doc.Object, doc.SpecialColumnsValues, references, "", doc.Attachments);

                            return (patcher.Patch(doc.ToBlittable(context)), id);
                        }
                    } 
                    catch (JavaScriptException e)
                    {
                        if (e.InnerException is Jint.Runtime.JavaScriptException innerException && string.Equals(innerException.Message, "skip", StringComparison.OrdinalIgnoreCase))
                        {
                            throw new InvalidOperationException("Document was skipped", e);
                        } 
                        else
                        {
                            throw;
                        }             
                    }
                    finally
                    {
                        DisposeDataProviders(references);
                    }
                }
            }
            throw new InvalidOperationException("Unable to find document to test. Query returned empty result.");
        }
        
        public async Task Migrate(MigrationSettings settings, DatabaseSchema dbSchema, DocumentDatabase db, DocumentsOperationContext context,
            MigrationResult result, Action<IOperationProgress> onProgress, CancellationToken token = default)
        {
            if (result == null)
                result = new MigrationResult(settings);

            if (onProgress == null)
                onProgress = progress => { };

            string CollectionNameProvider(string tableSchema, string tableName) => settings.Collections.Single(x => x.SourceTableSchema == tableSchema && x.SourceTableName == tableName).Name;

            await using (var enumerationConnection = OpenConnection())
            await using (var referencesConnection = OpenConnection())
            using (var writer = new SqlMigrationWriter(context, settings.BatchSize))
            {
                foreach (var collectionToImport in settings.Collections)
                {
                    var collectionCount = result.PerCollectionCount[collectionToImport.Name];

                    result.AddInfo($"Started processing collection {collectionToImport.Name}.");
                    onProgress.Invoke(result.Progress);

                    var tableSchema = dbSchema.GetTable(collectionToImport.SourceTableSchema, collectionToImport.SourceTableName);
                    var specialColumns = dbSchema.FindSpecialColumns(collectionToImport.SourceTableSchema, collectionToImport.SourceTableName);

                    using (var patcher = new JsPatcher(collectionToImport, context))
                    {
                        var references = ResolveReferences(collectionToImport, dbSchema, CollectionNameProvider);

                        InitializeDataProviders(references, referencesConnection);

                        try
                        {
                            foreach (var doc in EnumerateTable(GetQueryForCollection(collectionToImport), collectionToImport.ColumnsMapping, specialColumns,
                                collectionToImport.AttachmentNameMapping, enumerationConnection, settings.MaxRowsPerTable))
                            {
                                token.ThrowIfCancellationRequested();
                                
                                try
                                {
                                    collectionCount.ReadCount++;

                                    if (collectionCount.ReadCount % 1000 == 0)
                                    {
                                        var message = $"Read {collectionCount.ReadCount:#,#;;0} rows from table: " + collectionToImport.SourceTableName;
                                        result.AddInfo(message);
                                        onProgress.Invoke(result.Progress);
                                    }
                                    
                                    doc.SetCollection(collectionToImport.Name);

                                    var id = GenerateDocumentId(doc.Collection, GetColumns(doc.SpecialColumnsValues, tableSchema.PrimaryKeyColumns));

                                    FillDocumentFields(doc.Object, doc.SpecialColumnsValues, references, "", doc.Attachments);

                                    var docBlittable = patcher.Patch(doc.ToBlittable(context));

                                    await writer.InsertDocument(docBlittable, id, doc.Attachments);
                                }
                                catch (JavaScriptException e)
                                {
                                    if (e.InnerException is Jint.Runtime.JavaScriptException innerException && string.Equals(innerException.Message, "skip", StringComparison.OrdinalIgnoreCase))
                                    {
                                        collectionCount.SkippedCount++;
                                    } 
                                    else
                                    {
                                        throw;
                                    }
                                }
                                catch (Exception e)
                                {
                                    result.AddError(e.Message);
                                    collectionCount.ErroredCount++;
                                }
                            }
                        }
                        finally
                        {
                            DisposeDataProviders(references);
                        }
                    }

                    collectionCount.Processed = true;
                    result.AddInfo($"Finished processing collection {collectionToImport.Name}. {collectionCount}");
                    onProgress(result.Progress);
                }
            }
        }

        private void FillDocumentFields(DynamicJsonValue value, DynamicJsonValue specialColumns, List<ReferenceInformation> references,
            string attachmentNamePrefix, Dictionary<string, byte[]> attachments)
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
                                string innerAttachmentPrefix = GenerateAttachmentKey(attachmentNamePrefix, refInfo.PropertyName, idx.ToString());
                                FillDocumentFields(arrayItem, arrayWithEmbeddedObjects.SpecialColumnsValues[idx], refInfo.ChildReferences, innerAttachmentPrefix,
                                    attachments);

                                foreach (var kvp in arrayWithEmbeddedObjects.Attachments[idx])
                                {
                                    attachments[GenerateAttachmentKey(innerAttachmentPrefix, kvp.Key)] = kvp.Value;
                                }

                                idx++;
                            }
                        }

                        break;
                    case ReferenceType.ObjectEmbed:
                        var embeddedObjectValue = (EmbeddedObjectValue)refInfo.DataProvider.Provide(specialColumns);
                        value[refInfo.PropertyName] = embeddedObjectValue?.Object; // fill in value or null

                        if (embeddedObjectValue != null)
                        {
                            var innerAttachmentPrefix = GenerateAttachmentKey(attachmentNamePrefix, refInfo.PropertyName);
                            foreach (var kvp in embeddedObjectValue.Attachments)
                            {
                                attachments[GenerateAttachmentKey(innerAttachmentPrefix, kvp.Key)] = kvp.Value;
                            }

                            if (refInfo.ChildReferences != null)
                            {
                                FillDocumentFields(embeddedObjectValue.Object, embeddedObjectValue.SpecialColumnsValues, refInfo.ChildReferences, innerAttachmentPrefix,
                                    attachments);
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

        private string GenerateAttachmentKey(params string[] tokens)
        {
            return string.Join("_", tokens.Where(x => string.IsNullOrWhiteSpace(x) == false));
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

        private void InitializeDataProviders(List<ReferenceInformation> references, DbConnection connection)
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

        private List<ReferenceInformation> ResolveReferences(CollectionWithReferences sourceCollection, DatabaseSchema dbSchema, 
            Func<string, string, string> collectionNameProvider)
        {
            var result = new List<ReferenceInformation>();

            if (sourceCollection.NestedCollections != null)
            {
                foreach (var embeddedCollection in sourceCollection.NestedCollections)
                {
                    var reference = CreateReference(dbSchema, collectionNameProvider, sourceCollection, embeddedCollection);
                    var resolvedReferences = ResolveReferences(embeddedCollection, dbSchema, collectionNameProvider);
                    reference.ChildReferences = resolvedReferences.Count > 0 ? resolvedReferences : null;
                    result.Add(reference);
                }
            }

            if (sourceCollection.LinkedCollections != null)
            {
                foreach (var linkedCollection in sourceCollection.LinkedCollections)
                    result.Add(CreateReference(dbSchema, collectionNameProvider, sourceCollection, linkedCollection));
            }

            return result;
        }

        private ReferenceInformation CreateReference(DatabaseSchema dbSchema,
            Func<string, string, string> collectionNameProvider, AbstractCollection sourceCollection,
            ICollectionReference destinationCollection)
        {
            var sourceSchema = dbSchema.GetTable(sourceCollection.SourceTableSchema, sourceCollection.SourceTableName);
            var destinationSchema = dbSchema.GetTable(destinationCollection.SourceTableSchema, destinationCollection.SourceTableName);

            var reference = destinationCollection.Type == RelationType.OneToMany
                ? sourceSchema.FindReference((AbstractCollection)destinationCollection, destinationCollection.JoinColumns)
                : destinationSchema.FindReference(sourceCollection, destinationCollection.JoinColumns);

            if (reference == null)
            {
                throw new InvalidOperationException("Unable to resolve reference: " + sourceCollection.SourceTableName + " -> " + destinationCollection.SourceTableName
                                                    + ". Columns: " + string.Join(", ", destinationCollection.JoinColumns));
            }

            var specialColumns = dbSchema.FindSpecialColumns(destinationCollection.SourceTableSchema, destinationCollection.SourceTableName);

            var referenceInformation = new ReferenceInformation
            {
                SourcePrimaryKeyColumns = sourceSchema.PrimaryKeyColumns,
                SourceTableName = destinationCollection.SourceTableName,
                SourceSchema = destinationCollection.SourceTableSchema,
                TargetPrimaryKeyColumns = destinationSchema.PrimaryKeyColumns,
                PropertyName = destinationCollection.Name,
                ForeignKeyColumns = reference.Columns,
                TargetDocumentColumns = destinationCollection.ColumnsMapping,
                TargetSpecialColumnsNames = specialColumns,
                TargetAttachmentColumns = destinationCollection.AttachmentNameMapping,
                Type = (destinationCollection is EmbeddedCollection)
                    ? (destinationCollection.Type == RelationType.ManyToOne ? ReferenceType.ObjectEmbed : ReferenceType.ArrayEmbed)
                    : (destinationCollection.Type == RelationType.ManyToOne ? ReferenceType.ObjectLink : ReferenceType.ArrayLink),
            };

            if (destinationCollection is LinkedCollection)
            {
                // linked connection
                referenceInformation.CollectionNameToUseInLinks = collectionNameProvider(destinationCollection.SourceTableSchema, destinationCollection.SourceTableName);
            }

            return referenceInformation;
        }

        protected Dictionary<string, byte[]> ExtractAttachments(IDataReader reader, Dictionary<string, string> attachmentNameMapping)
        {
            var result = new Dictionary<string, byte[]>();

            foreach (var attachmentKvp in attachmentNameMapping)
            {
                var value = reader[attachmentKvp.Key];
                if (value != null && (value is DBNull) == false)
                {
                    result[attachmentKvp.Value] = (byte[])value;
                }
            }

            return result;
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

        protected DynamicJsonValue ExtractFromReader(DbDataReader reader, Dictionary<string, string> columnsMapping)
        {
            var document = new DynamicJsonValue();

            foreach (var kvp in columnsMapping)
            {
                document[kvp.Value] = ExtractValue(reader[kvp.Key]);
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

        protected abstract string LimitRowsNumber(string inputQuery, int? rowsLimit);
        
        protected abstract string GetSelectAllQueryForTable(string tableSchema, string tableName);

        protected abstract string QuoteTable(string schema, string tableName);
        protected abstract string QuoteColumn(string columnName);
        protected abstract string FactoryName { get; }

        protected IDataProvider<string> CreateObjectLinkDataProvider(ReferenceInformation refInfo)
        {
            return new LocalDataProvider<string>(specialColumns =>
                GenerateDocumentId(refInfo.CollectionNameToUseInLinks, GetColumns(specialColumns, refInfo.ForeignKeyColumns)));
        }

        protected DbConnection OpenConnection()
        {
            var factory = DbProviderFactories.GetFactory(FactoryName);
            DbConnection connection;
            try
            {
                connection = factory.CreateConnection();
                connection.ConnectionString = ConnectionString;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Cannot create new sql connection using the given connection string", e);
            }

            try
            {
                connection.Open();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Cannot open connection using the given connection string", e);
            }


            return connection;
        }

        protected virtual string GetQueryByPrimaryKey(RootCollection collection, SqlTableSchema tableSchema, string[] primaryKeyValues, out Dictionary<string, object> queryParameters)
        {
            var primaryKeyColumns = tableSchema.PrimaryKeyColumns;
            if (primaryKeyColumns.Count != primaryKeyValues.Length)
            {
                queryParameters = null;
                throw new InvalidOperationException("Invalid parameters count. Primary key has " + primaryKeyColumns.Count + " columns, but " + primaryKeyValues.Length + " values were provided.");
            }
            
            var parameters = new Dictionary<string, object>();
            
            var queryParametersAsString = string.Join(" and ", primaryKeyColumns.Select((column, idx) =>
            {
                parameters["p" + idx] = ValueAsObject(tableSchema, column, primaryKeyValues, idx);
                return $"{QuoteColumn(column)} = @p{idx}";
            }));
            
            queryParameters = parameters;
            
            // here we ignore custom query - as we want to get row based on primary key
            return $"select * from {QuoteTable(collection.SourceTableSchema, collection.SourceTableName)} where {queryParametersAsString}";
        }

        public object ValueAsObject(SqlTableSchema tableSchema, string column, string[] primaryKeyValue, int index)
        {
            var type = tableSchema.Columns.Find(x => x.Name == column).Type;
            var value = type == ColumnType.Number ? (object)int.Parse(primaryKeyValue[index].ToString()) : primaryKeyValue[index];

            return value;
        }

        protected IEnumerable<SqlMigrationDocument> EnumerateTable(string tableQuery, Dictionary<string, string> documentPropertiesMapping, 
            HashSet<string> specialColumns, Dictionary<string, string> attachmentNameMapping, DbConnection connection, int? rowsLimit, Dictionary<string, object> queryParameters = null)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = LimitRowsNumber(tableQuery, rowsLimit);
                
                if (queryParameters != null)
                {
                    foreach (var kvp in queryParameters)
                    {
                        DbParameter dbParameter = cmd.CreateParameter();
                        dbParameter.ParameterName = kvp.Key;
                        dbParameter.Value = kvp.Value;

                        cmd.Parameters.Add(dbParameter);
                    }
                }
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var migrationDocument = new SqlMigrationDocument
                        {
                            Object = ExtractFromReader(reader, documentPropertiesMapping),
                            SpecialColumnsValues = ExtractFromReader(reader, specialColumns),
                            Attachments = ExtractAttachments(reader, attachmentNameMapping)
                        };
                        yield return migrationDocument;
                    }
                }
            }
        }
        
        protected virtual IDataProvider<DynamicJsonArray> CreateArrayLinkDataProvider(ReferenceInformation refInfo, DbConnection connection)
        {
            var queryColumns = string.Join(", ", refInfo.TargetPrimaryKeyColumns.Select(QuoteColumn));
            var queryParameters = string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));
            var query = $"select {queryColumns} from {QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)} where {queryParameters}";


            return new SqlStatementProvider<DynamicJsonArray>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
            {
                var result = new DynamicJsonArray();
                while (reader.Read())
                {
                    var linkParameters = new object[reader.FieldCount];
                    for (var i = 0; i < linkParameters.Length; i++)
                    {
                        linkParameters[i] = reader[i];
                    }

                    result.Add(GenerateDocumentId(refInfo.CollectionNameToUseInLinks, linkParameters));
                }

                return result;
            });
        }

        protected virtual IDataProvider<EmbeddedObjectValue> CreateObjectEmbedDataProvider(ReferenceInformation refInfo, DbConnection connection)
        {
            var queryParameters = string.Join(" and ", refInfo.TargetPrimaryKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));
            var query = $"select * from {QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)} where {queryParameters}";

            return new SqlStatementProvider<EmbeddedObjectValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.ForeignKeyColumns), reader =>
            {
                if (reader.Read() == false)
                {
                    // parent object is null
                    return new EmbeddedObjectValue();
                }

                return new EmbeddedObjectValue
                {
                    Object = ExtractFromReader(reader, refInfo.TargetDocumentColumns),
                    SpecialColumnsValues = ExtractFromReader(reader, refInfo.TargetSpecialColumnsNames),
                    Attachments = ExtractAttachments(reader, refInfo.TargetAttachmentColumns)
                };
            });
        }

        protected virtual IDataProvider<EmbeddedArrayValue> CreateArrayEmbedDataProvider(ReferenceInformation refInfo, DbConnection connection)
        {
            var queryParameters = string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));
            var query = $"select * from {QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)} where {queryParameters}";

            return new SqlStatementProvider<EmbeddedArrayValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
            {
                var objectProperties = new DynamicJsonArray();
                var specialProperties = new List<DynamicJsonValue>();
                var attachments = new List<Dictionary<string, byte[]>>();
                while (reader.Read())
                {
                    objectProperties.Add(ExtractFromReader(reader, refInfo.TargetDocumentColumns));
                    attachments.Add(ExtractAttachments(reader, refInfo.TargetAttachmentColumns));
                    
                    if (refInfo.ChildReferences != null)
                    {
                        // fill only when used
                        specialProperties.Add(ExtractFromReader(reader, refInfo.TargetSpecialColumnsNames));    
                        
                    }
                }

                return new EmbeddedArrayValue
                {
                    ArrayOfNestedObjects = objectProperties,
                    SpecialColumnsValues = specialProperties,
                    Attachments = attachments
                };
            });
        }
        
        protected string GetQueryForCollection(RootCollection collection)
        {
            if (string.IsNullOrWhiteSpace(collection.SourceTableQuery) == false)
            {
                return collection.SourceTableQuery;
            }

            return GetSelectAllQueryForTable(collection.SourceTableSchema, collection.SourceTableName);
        }
    }
}

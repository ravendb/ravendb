//-----------------------------------------------------------------------
// <copyright file="IDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Net.Http;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client.Changes;
using Raven.Client.Connection.Implementation;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
    /// <summary>
    ///     A sync database command operations
    /// </summary>
    public interface IDatabaseCommands : IHoldProfilingInformation
    {
        /// <summary>
        ///     Admin operations for current database
        /// </summary>
        IAdminDatabaseCommands Admin { get; }

        /// <summary>
        ///     Admin operations performed against system database, like create/delete database
        /// </summary>
        IGlobalAdminDatabaseCommands GlobalAdmin { get; }

        /// <summary>
        ///     Info operations for current database
        /// </summary>
        IInfoDatabaseCommands Info { get; }

        /// <summary>
        ///     Gets or sets the operations headers
        /// </summary>
        NameValueCollection OperationsHeaders { get; set; }

        /// <summary>
        ///     Primary credentials for access. Will be used also in replication context - for failovers
        /// </summary>
        OperationCredentials PrimaryCredentials { get; }

        /// <summary>
        ///     Sends multiple operations in a single request, reducing the number of remote calls and allowing several operations
        ///     to share same transaction
        /// </summary>
        /// <param name="commandDatas">Commands to process</param>
        /// <param name="options">Options to send to the server</param>
        BatchResult[] Batch(IEnumerable<ICommandData> commandDatas, BatchOptions options = null);

#if !DNXCORE50
        /// <summary>
        ///     Commits the specified tx id
        /// </summary>
        /// <param name="txId">transaction identifier</param>
        void Commit(string txId);
#endif

        HttpJsonRequest CreateReplicationAwareRequest(string currentServerUrl, string requestUrl, HttpMethod method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null);

        /// <summary>
        ///     Create a http request to the specified relative url on the current database
        /// </summary>
        HttpJsonRequest CreateRequest(string relativeUrl, HttpMethod method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null);

        /// <summary>
        ///     Deletes the document with the specified key
        /// </summary>
        /// <param name="key">key of a document to be deleted</param>
        /// <param name="etag">current document etag, used for concurrency checks (null to skip check)</param>
        void Delete(string key, Etag etag);

        /// <summary>
        ///     Removes an attachment from a database.
        /// </summary>
        /// <param name="key">key of an attachment to delete</param>
        /// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
        [Obsolete("Use RavenFS instead.")]
        void DeleteAttachment(string key, Etag etag);

        /// <summary>
        ///     Perform a set based deletes using the specified index
        /// </summary>
        /// <param name="indexName">name of an index to perform a query on</param>
        /// <param name="queryToDelete">Tquery that will be performed</param>
        /// <param name="options">various operation options e.g. AllowStale or MaxOpsPerSec</param>
        Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, BulkOperationOptions options = null);

        /// <summary>
        ///     Deletes the specified index
        /// </summary>
        /// <param name="name">name of an index to delete</param>
        void DeleteIndex(string name);

        /// <summary>
        ///     Deletes the specified transformer
        /// </summary>
        /// <param name="name">name of a transformer to delete</param>
        void DeleteTransformer(string name);

        /// <summary>
        ///     Disable all caching within the given scope
        /// </summary>
        IDisposable DisableAllCaching();

        /// <summary>
        ///     Create a new instance of <see cref="IDatabaseCommands" /> that will interact
        ///     with the specified database
        /// </summary>
        IDatabaseCommands ForDatabase(string database);

        /// <summary>
        ///     Create a new instance of <see cref="IDatabaseCommands" /> that will interact
        ///     with the default database
        /// </summary>
        IDatabaseCommands ForSystemDatabase();

        /// <summary>
        ///     Force the database commands to read directly from the master, unless there has been a failover.
        /// </summary>
        IDisposable ForceReadFromMaster();

        /// <summary>
        ///     Retrieve a single document for a specified key.
        /// </summary>
        /// <param name="key">key of the document you want to retrieve</param>
        JsonDocument Get(string key);

        /// <summary>
        ///     Retrieves documents with the specified ids, optionally specifying includes to fetch along and also optionally the
        ///     transformer.
        ///     <para>Returns MultiLoadResult where:</para>
        ///     <para>- Results - list of documents in exact same order as in keys parameter</para>
        ///     <para>- Includes - list of documents that were found in specified paths that were passed in includes parameter</para>
        /// </summary>
        /// <param name="ids">array of keys of the documents you want to retrieve</param>
        /// <param name="includes">array of paths in documents in which server should look for a 'referenced' document</param>
        /// <param name="transformer">name of a transformer that should be used to transform the results</param>
        /// <param name="transformerParameters">parameters that will be passed to transformer</param>
        /// <param name="metadataOnly">specifies if only document metadata should be returned</param>
        MultiLoadResult Get(string[] ids, string[] includes, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, bool metadataOnly = false);

        /// <summary>
        ///     Downloads a single attachment.
        /// </summary>
        /// <param name="key">key of the attachment you want to download</param>
        [Obsolete("Use RavenFS instead.")]
        Attachment GetAttachment(string key);

        /// <summary>
        ///     Downloads attachment metadata for a multiple attachments.
        /// </summary>
        /// <param name="idPrefix">prefix for which attachments should be returned</param>
        /// <param name="start">number of attachments that should be skipped</param>
        /// <param name="pageSize">maximum number of attachments that will be returned</param>
        [Obsolete("Use RavenFS instead.")]
        IEnumerable<Attachment> GetAttachmentHeadersStartingWith(string idPrefix, int start, int pageSize);

        /// <summary>
        ///     Used to download attachment information for multiple attachments.
        /// </summary>
        /// <param name="start">indicates how many attachments should be skipped</param>
        /// <param name="startEtag">ETag from which to start</param>
        /// <param name="pageSize">maximum number of attachments that will be downloaded</param>
        [Obsolete("Use RavenFS instead.")]
        AttachmentInformation[] GetAttachments(int start, Etag startEtag, int pageSize);

        /// <summary>
        ///     Get the low level bulk insert operation
        /// </summary>
        ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes);

        /// <summary>
        ///     Retrieves multiple documents.
        /// </summary>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="metadataOnly">specifies if only document metadata should be returned</param>
        /// <remarks>
        ///     This is primarily useful for administration of a database
        /// </remarks>
        JsonDocument[] GetDocuments(int start, int pageSize, bool metadataOnly = false);

        /// <summary>
        ///     Retrieves multiple documents.
        /// </summary>
        /// <param name="fromEtag">Etag from which documents should start</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="metadataOnly">specifies if only document metadata should be returned</param>
        /// <remarks>
        ///     This is primarily useful for administration of a database
        /// </remarks>
        JsonDocument[] GetDocuments(Etag fromEtag, int pageSize, bool metadataOnly = false);

        /// <summary>
        ///     Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
        /// </summary>
        /// <param name="index">name of an index to query</param>
        /// <param name="query">query definition containing all information required to query a specified index</param>
        /// <param name="facetSetupDoc">document key that contains predefined FacetSetup</param>
        /// <param name="start">number of results that should be skipped. Default: 0</param>
        /// <param name="pageSize">
        ///     maximum number of results that will be retrieved. Default: null. If set, overrides
        ///     Facet.MaxResults
        /// </param>
        FacetResults GetFacets(string index, IndexQuery query, string facetSetupDoc, int start = 0, int? pageSize = null);

        /// <summary>
        ///     Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
        /// </summary>
        /// <param name="index">name of an index to query</param>
        /// <param name="query">query definition containing all information required to query a specified index</param>
        /// <param name="facets">list of facets required to perform a facet query</param>
        /// <param name="start">number of results that should be skipped. Default: 0</param>
        /// <param name="pageSize">
        ///     maximum number of results that will be retrieved. Default: null. If set, overrides
        ///     Facet.MaxResults
        /// </param>
        FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets, int start = 0, int? pageSize = null);

        /// <summary>
        ///     Retrieves an index definition from a database.
        /// </summary>
        /// <param name="name">name of an index</param>
        IndexDefinition GetIndex(string name);

        /// <summary>
        ///     Retrieves indexing performance statistics for all indexes
        /// </summary>
        IndexingPerformanceStatistics[] GetIndexingPerformanceStatistics();

        /// <summary>
        ///     Retrieves all suggestions for an index merging
        /// </summary>
        IndexMergeResults GetIndexMergeSuggestions();

        /// <summary>
        ///     Retrieves multiple index names from a database.
        /// </summary>
        /// <param name="start">number of index names that should be skipped</param>
        /// <param name="pageSize">maximum number of index names that will be retrieved</param>
        string[] GetIndexNames(int start, int pageSize);

        /// <summary>
        ///     Retrieves multiple index definitions from a database
        /// </summary>
        /// <param name="start">number of indexes that should be skipped</param>
        /// <param name="pageSize">maximum number of indexes that will be retrieved</param>
        IndexDefinition[] GetIndexes(int start, int pageSize);

        /// <summary>
        ///     Gets the license status
        /// </summary>
        LicensingStatus GetLicenseStatus();

        /// <summary>
        ///     Gets the Logs
        /// </summary>
        LogItem[] GetLogs(bool errorsOnly);

        /// <summary>
        ///     Sends a multiple faceted queries in a single request and calculates the facet results for each of them
        /// </summary>
        /// <param name="facetedQueries">List of the faceted queries that will be executed on the server-side</param>
        FacetResults[] GetMultiFacets(FacetQuery[] facetedQueries);

        /// <summary>
        ///     Retrieve the statistics for the database
        /// </summary>
        DatabaseStatistics GetStatistics();

        /// <summary>
        ///     Retrieve the user info
        /// </summary>

        UserInfo GetUserInfo();

        /// <summary>
        ///     Retrieves user permissions for a specified database
        /// </summary>
        /// <param name="database">name of the database we want to retrive the permissions</param>
        /// <param name="readOnly">the type of the operations allowed, read only , or read-write</param>
        UserPermission GetUserPermission(string database, bool readOnly);

        /// <summary>
        ///     Get the all terms stored in the index for the specified field
        ///     You can page through the results by use fromValue parameter as the
        ///     starting point for the next query
        /// </summary>
        /// <param name="index">name of an index</param>
        /// <param name="field">index field</param>
        /// <param name="fromValue">starting point for a query, used for paging</param>
        /// <param name="pageSize">maximum number of terms that will be returned</param>
        IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize);

        /// <summary>
        ///     Gets the transformer definition for the specified name
        /// </summary>
        /// <param name="name">transformer name</param>
        TransformerDefinition GetTransformer(string name);

        /// <summary>
        ///     Gets the transformers from the server
        /// </summary>
        /// <param name="start">number of transformers that should be skipped</param>
        /// <param name="pageSize">maximum number of transformers that will be retrieved</param>
        TransformerDefinition[] GetTransformers(int start, int pageSize);

        /// <summary>
        /// Sets the transformer's lock mode
        /// </summary>
        /// <param name="name">The name of the transformer</param>
        /// <param name="lockMode">The lock mode to be set</param>
        void SetTransformerLock(string name, TransformerLockMode lockMode);

        /// <summary>
        ///     Retrieves the document metadata for the specified document key.
        ///     <para>Returns:</para>
        ///     <para>The document metadata for the specified document, or <c>null</c> if the document does not exist</para>
        /// </summary>
        /// <param name="key">key of a document to get metadata for</param>
        /// <returns>The document metadata for the specified document, or null if the document does not exist</returns>
        JsonDocumentMetadata Head(string key);

        /// <summary>
        ///     Download attachment metadata for a single attachment.
        /// </summary>
        /// <param name="key">key of the attachment you want to download metadata for</param>
        [Obsolete("Use RavenFS instead.")]
        Attachment HeadAttachment(string key);

        /// <summary>
        ///     Lets you check if the given index definition differs from the one on a server.
        ///     <para>
        ///         This might be useful when you want to check the prior index deployment, if index will be overwritten, and if
        ///         indexing data will be lost.
        ///     </para>
        ///     <para>Returns:</para>
        ///     <para>- <c>true</c> - if an index does not exist on a server</para>
        ///     <para>- <c>true</c> - if an index definition does not match the one from the indexDef parameter,</para>
        ///     <para>
        ///         - <c>false</c> - if there are no differences between an index definition on server and the one from the
        ///         indexDef parameter
        ///     </para>
        ///     If index does not exist this method returns true.
        /// </summary>
        /// <param name="name">name of an index to check</param>
        /// <param name="indexDef">index definition</param>
        bool IndexHasChanged(string name, IndexDefinition indexDef);

        /// <summary>
        ///     Return a list of documents that based on the MoreLikeThisQuery.
        /// </summary>
        /// <param name="query">more like this query definition that will be executed</param>
        MultiLoadResult MoreLikeThis(MoreLikeThisQuery query);

        /// <summary>
        ///     Perform a single POST request containing multiple nested GET requests
        /// </summary>
        GetResponse[] MultiGet(GetRequest[] requests);

        /// <summary>
        ///     Generate the next identity value from the server
        /// </summary>
        long NextIdentityFor(string name);

        /// <summary>
        ///     Sends a patch request for a specific document, ignoring the document's Etag and if the document is missing
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patches">Array of patch requests</param>
        RavenJObject Patch(string key, PatchRequest[] patches);

        /// <summary>
        ///     Sends a patch request for a specific document, ignoring the document's Etag
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patches">Array of patch requests</param>
        /// <param name="ignoreMissing">
        ///     true if the patch request should ignore a missing document, false to throw
        ///     DocumentDoesNotExistException
        /// </param>
        RavenJObject Patch(string key, PatchRequest[] patches, bool ignoreMissing);

        /// <summary>
        ///     Sends a patch request for a specific document, ignoring the document's Etag and  if the document is missing
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
        RavenJObject Patch(string key, ScriptedPatchRequest patch);

        /// <summary>
        ///     Sends a patch request for a specific document, ignoring the document's Etag
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
        /// <param name="ignoreMissing">
        ///     true if the patch request should ignore a missing document, false to throw
        ///     DocumentDoesNotExistException
        /// </param>
        RavenJObject Patch(string key, ScriptedPatchRequest patch, bool ignoreMissing);

        /// <summary>
        ///     Sends a patch request for a specific document
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patches">Array of patch requests</param>
        /// <param name="etag">Require specific Etag [null to ignore]</param>
        RavenJObject Patch(string key, PatchRequest[] patches, Etag etag);

        /// <summary>
        ///     Sends a patch request for a specific document which may or may not currently exist
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
        /// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
        /// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
        RavenJObject Patch(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata);

        /// <summary>
        ///     Sends a patch request for a specific document
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
        /// <param name="etag">Require specific Etag [null to ignore]</param>
        RavenJObject Patch(string key, ScriptedPatchRequest patch, Etag etag);

        /// <summary>
        ///     Sends a patch request for a specific document which may or may not currently exist
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patchExisting">The patch request to use (using JavaScript) to an existing document</param>
        /// <param name="patchDefault">
        ///     The patch request to use (using JavaScript)  to a default document when the document is
        ///     missing
        /// </param>
        /// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
        RavenJObject Patch(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata);

#if !DNXCORE50
        /// <summary>
        ///     Prepares the transaction on the server.
        /// </summary>
        void PrepareTransaction(string txId, Guid? resourceManagerId = null, byte[] recoveryInformation = null);
#endif

        /// <summary>
        ///     Puts the document in the database with the specified key.
        ///     <para>Returns PutResult where:</para>
        ///     <para>- Key - unique key under which document was stored,</para>
        ///     <para>- Etag - stored document etag</para>
        /// </summary>
        /// <param name="key">unique key under which document will be stored</param>
        /// <param name="etag">current document etag, used for concurrency checks (null to skip check)</param>
        /// <param name="document">document data</param>
        /// <param name="metadata">document metadata</param>
        PutResult Put(string key, Etag etag, RavenJObject document, RavenJObject metadata);

        /// <summary>
        ///     Puts a byte array as attachment with the specified key
        /// </summary>
        /// <param name="key">unique key under which attachment will be stored</param>
        /// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
        /// <param name="data">attachment data</param>
        /// <param name="metadata">attachment metadata</param>
        [Obsolete("Use RavenFS instead.")]
        void PutAttachment(string key, Etag etag, Stream data, RavenJObject metadata);

        /// <summary>
        ///     Creates an index with the specified name, based on an index definition
        /// </summary>
        /// <param name="name">name of an index</param>
        /// <param name="indexDef">definition of an index</param>
        string PutIndex(string name, IndexDefinition indexDef);

        /// <summary>
        ///      Creates multiple indexes with the specified name, using given index definitions and priorities
        /// </summary>
        /// <param name="indexesToAdd">indexes to add</param>
        string[] PutIndexes(IndexToAdd[] indexesToAdd);

        /// <summary>
        ///      Creates multiple side by side indexes with the specified name, using given index definitions and priorities
        /// </summary>
        /// <param name="indexesToAdd">indexes to add</param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        /// <param name="replaceTimeUtc">The minimum time after which indexes will be swapped.</param>
        string[] PutSideBySideIndexes(IndexToAdd[] indexesToAdd, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null);

        /// <summary>
        ///     Creates an index with the specified name, based on an index definition
        /// </summary>
        /// <param name="name">name of an index</param>
        /// <param name="indexDef">definition of an index</param>
        /// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
        string PutIndex(string name, IndexDefinition indexDef, bool overwrite);

        /// <summary>
        ///     Creates an index with the specified name, based on an index definition that is created by the supplied
        ///     IndexDefinitionBuilder
        /// </summary>
        /// <typeparam name="TDocument">Type of the document index should work on</typeparam>
        /// <typeparam name="TReduceResult">Type of reduce result</typeparam>
        /// <param name="name">name of an index</param>
        /// <param name="indexDef">definition of an index</param>
        string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef);

        /// <summary>
        ///     Creates an index with the specified name, based on an index definition that is created by the supplied
        ///     IndexDefinitionBuilder
        /// </summary>
        /// <typeparam name="TDocument">Type of the document index should work on</typeparam>
        /// <typeparam name="TReduceResult">Type of reduce result</typeparam>
        /// <param name="name">name of an index</param>
        /// <param name="indexDef">definition of an index</param>
        /// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
        string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite);

        /// <summary>
        ///     Creates a transformer with the specified name, based on an transformer definition
        /// </summary>
        /// <param name="name">name of a transformer</param>
        /// <param name="transformerDefinition">definition of a transformer</param>
        string PutTransformer(string name, TransformerDefinition transformerDefinition);

        /// <summary>
        ///     Queries the specified index in the Raven-flavored Lucene query syntax
        /// </summary>
        /// <param name="index">name of an index to query</param>
        /// <param name="query">query definition containing all information required to query a specified index</param>
        /// <param name="includes">
        ///     an array of relative paths that specify related documents ids which should be included in a
        ///     query result
        /// </param>
        /// <param name="metadataOnly">true if returned documents should include only metadata without a document body.</param>
        /// <param name="indexEntriesOnly">true if query results should contain only index entries.</param>
        QueryResult Query(string index, IndexQuery query, string[] includes = null, bool metadataOnly = false, bool indexEntriesOnly = false);

        /// <summary>
        ///     Removes all indexing data from a server for a given index so the indexation can start from scratch for that index.
        /// </summary>
        /// <param name="name">name of an index to reset</param>
        void ResetIndex(string name);


        void SetIndexLock(string name, IndexLockMode unlock);

        void SetIndexPriority(string name, IndexingPriority priority);

#if !DNXCORE50
        /// <summary>
        ///     Rollbacks the specified tx id
        /// </summary>
        /// <param name="txId">transaction identifier</param>
        void Rollback(string txId);
#endif

        /// <summary>
        ///     Seeds the next identity value on the server
        /// </summary>
        long SeedIdentityFor(string name, long value);

        /// <summary>
        ///     Seeds the next identities value on the server
        /// </summary>
        /// <param name="identities"></param>
        void SeedIdentities(List<KeyValuePair<string, long>> identities);

        /// <summary>
        ///     Retrieves documents for the specified key prefix.
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="metadataOnly">specifies if only document metadata should be returned</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="transformer">name of a transformer that should be used to transform the results</param>
        /// <param name="transformerParameters">parameters that will be passed to transformer</param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize, RavenPagingInformation pagingInformation = null, bool metadataOnly = false, string exclude = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, string skipAfter = null);

        /// <summary>
        ///     Streams the documents by etag OR starts with the prefix and match the matches
        ///     Will return *all* results, regardless of the number of itmes that might be returned.
        /// </summary>
        /// <param name="fromEtag">ETag of a document from which stream should start (mutually exclusive with 'startsWith')</param>
        /// <param name="startsWith">prefix for which documents should be streamed (mutually exclusive with 'fromEtag')</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        /// <param name="transformer">name of a transformer that should be used to transform the results</param>
        /// <param name="transformerParameters">parameters that will be passed to transformer</param>
        IEnumerator<RavenJObject> StreamDocs(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0, int pageSize = int.MaxValue, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null);

        /// <summary>
        ///     Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
        ///     of the number of items that might be returned.
        /// </summary>
        /// <param name="index">name of an index to query</param>
        /// <param name="query">query definition containing all information required to query a specified index</param>
        /// <param name="queryHeaderInfo">information about performed query</param>
        IEnumerator<RavenJObject> StreamQuery(string index, IndexQuery query, out QueryHeaderInformation queryHeaderInfo);

        /// <summary>
        ///     Returns a list of suggestions based on the specified suggestion query
        /// </summary>
        /// <param name="index">name of an index to query</param>
        /// <param name="suggestionQuery">
        ///     suggestion query definition containing all information required to query a specified
        ///     index
        /// </param>
        SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery);

        /// <summary>
        ///     Updates attachments metadata only.
        /// </summary>
        /// <param name="key">key under which attachment is stored</param>
        /// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
        /// <param name="metadata">attachment metadata</param>
        [Obsolete("Use RavenFS instead.")]
        void UpdateAttachmentMetadata(string key, Etag etag, RavenJObject metadata);

        /// <summary>
        ///     Perform a set based update using the specified index
        /// </summary>
        /// <param name="indexName">name of an index to perform a query on</param>
        /// <param name="queryToUpdate">query that will be performed</param>
        /// <param name="patchRequests">array of patches that will be executed on a query results</param>
        /// <param name="options">various operation options e.g. AllowStale or MaxOpsPerSec</param>
        Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, BulkOperationOptions options = null);

        /// <summary>
        ///     Perform a set based update using the specified index
        /// </summary>
        /// <param name="indexName">name of an index to perform a query on</param>
        /// <param name="queryToUpdate">query that will be performed</param>
        /// <param name="patch">JavaScript patch that will be executed on query results</param>
        /// <param name="options">various operation options e.g. AllowStale or MaxOpsPerSec</param>
        Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, BulkOperationOptions options = null);

        /// <summary>
        /// Gets the primary database Url
        /// </summary>
        string Url { get; }

        /// <summary>
        ///     Get the full URL for the given document key
        /// </summary>
        string UrlFor(string documentKey);

        /// <summary>
        ///     Returns a new <see cref="IDatabaseCommands" /> that use specified credentials
        /// </summary>
        /// <param name="credentialsForSession">credentials to use</param>
        IDatabaseCommands With(ICredentials credentialsForSession);
    }

    public interface IGlobalAdminDatabaseCommands
    {
        IDatabaseCommands Commands { get; }

        /// <summary>
        ///     Sends an async command to compact a database. During the compaction the specified database will be offline.
        /// </summary>
        /// <param name="databaseName">name of a database to compact</param>
        Operation CompactDatabase(string databaseName);

        /// <summary>
        ///     Creates a database
        /// </summary>
        void CreateDatabase(DatabaseDocument databaseDocument);

        /// <summary>
        ///     Used to delete a database from a server, with a possibility to remove all the data from hard drive.
        ///     <para>
        ///         Warning: if hardDelete is set to <c>true</c> then ALL data will be removed from the data directory of a
        ///         database.
        ///     </para>
        /// </summary>
        /// <param name="databaseName">name of a database to delete</param>
        /// <param name="hardDelete">should all data be removed (data files, indexing files, etc.). Default: false</param>
        void DeleteDatabase(string databaseName, bool hardDelete = false);

        /// <summary>
        ///     Ensures that the database exists, creating it if needed
        /// </summary>
        void EnsureDatabaseExists(string name, bool ignoreFailures = false);

        /// <summary>
        ///     Gets the build number
        /// </summary>
        BuildNumber GetBuildNumber();

        /// <summary>
        ///     Returns the names of all tenant databases on the RavenDB server
        /// </summary>
        string[] GetDatabaseNames(int pageSize, int start = 0);

        /// <summary>
        ///     Gets server-wide statistics.
        /// </summary>
        AdminStatistics GetStatistics();

        /// <summary>
        ///     Begins a backup operation.
        /// </summary>
        /// <param name="backupLocation">path to directory where backup will be stored</param>
        /// <param name="databaseDocument">
        ///     Database configuration document that will be stored with backup in 'Database.Document'
        ///     file. Pass <c>null</c> to use the one from system database. WARNING: Database configuration document may contain
        ///     sensitive data which will be decrypted and stored in backup.
        /// </param>
        /// <param name="incremental">indicates if backup is incremental</param>
        /// <param name="databaseName">name of a database that will be backed up</param>
        Operation StartBackup(string backupLocation, DatabaseDocument databaseDocument, bool incremental, string databaseName);

        /// <summary>
        ///     Begins a restore operation.
        /// </summary>
        Operation StartRestore(DatabaseRestoreRequest restoreRequest);
    }

    public interface IAdminDatabaseCommands
    {
        /// <summary>
        ///     Gets configuration for current database.
        /// </summary>
        RavenJObject GetDatabaseConfiguration();

        /// <summary>
        ///     Get the indexing status
        /// </summary>
        IndexingStatus GetIndexingStatus();

        /// <summary>
        ///     Enables indexing.
        /// </summary>
        /// <param name="maxNumberOfParallelIndexTasks">
        ///     if set then maximum number of parallel indexing tasks will be set to this
        ///     value.
        /// </param>
        void StartIndexing(int? maxNumberOfParallelIndexTasks = null);

        /// <summary>
        ///     Disables all indexing.
        /// </summary>
        void StopIndexing();
    }

    public interface IInfoDatabaseCommands
    {
        /// <summary>
        ///     Get replication info
        /// </summary>
        ReplicationStatistics GetReplicationInfo();
    }


    public class BatchOptions
    {
        public bool WaitForReplicas { get; set; }
        public int NumberOfReplicasToWaitFor { get; set; }
        public TimeSpan WaitForReplicasTimout { get; set; }

        public bool WaitForIndexes { get; set; }
        public TimeSpan WaitForIndexesTimeout { get; set; }
        public bool ThrowOnTimeoutInWaitForIndexes { get; set; }
    }

}

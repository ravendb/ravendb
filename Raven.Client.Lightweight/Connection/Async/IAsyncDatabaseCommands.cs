//-----------------------------------------------------------------------
// <copyright file="IAsyncDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection.Implementation;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Client.Connection.Async
{
    /// <summary>
    ///     An async database command operations
    /// </summary>
    public interface IAsyncDatabaseCommands : IDisposable, IHoldProfilingInformation
    {
        /// <summary>
        ///     Admin operations for current database
        /// </summary>
        IAsyncAdminDatabaseCommands Admin { get; }

        /// <summary>
        ///     Admin operations performed against system database, like create/delete database
        /// </summary>
        IAsyncGlobalAdminDatabaseCommands GlobalAdmin { get; }

        /// <summary>
        ///     Info operations for current database
        /// </summary>
        IAsyncInfoDatabaseCommands Info { get; }

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
        /// <param name="token">The cancellation token.</param>
        Task<BatchResult[]> BatchAsync(IEnumerable<ICommandData> commandDatas, CancellationToken token = default (CancellationToken));

#if !DNXCORE50
        /// <summary>
        ///     Commits the specified tx id
        /// </summary>
        /// <param name="txId">transaction identifier</param>
        /// <param name="token">The cancellation token.</param>
        Task CommitAsync(string txId, CancellationToken token = default (CancellationToken));
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
        /// <param name="token">The cancellation token.</param>
        Task DeleteAsync(string key, Etag etag, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Removes an attachment from a database.
        /// </summary>
        /// <param name="key">key of an attachment to delete</param>
        /// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
        /// <param name="token">The cancellation token.</param>
        [Obsolete("Use RavenFS instead.")]
        Task DeleteAttachmentAsync(string key, Etag etag, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Perform a set based deletes using the specified index
        /// </summary>
        /// <param name="indexName">name of an index to perform a query on</param>
        /// <param name="queryToDelete">Tquery that will be performed</param>
        /// <param name="options">various operation options e.g. AllowStale or MaxOpsPerSec</param>
        /// <param name="token">The cancellation token.</param>
        Task<Operation> DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, BulkOperationOptions options = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Deletes the specified index
        /// </summary>
        /// <param name="name">name of an index to delete</param>
        /// <param name="token">The cancellation token.</param>
        Task DeleteIndexAsync(string name, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Deletes the specified transformer
        /// </summary>
        /// <param name="name">name of a transformer to delete</param>
        /// <param name="token">The cancellation token.</param>
        Task DeleteTransformerAsync(string name, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Disable all caching within the given scope
        /// </summary>
        IDisposable DisableAllCaching();

        /// <summary>
        ///     Create a new instance of <see cref="IAsyncDatabaseCommands" /> that will interacts
        ///     with the specified database
        /// </summary>
        IAsyncDatabaseCommands ForDatabase(string database);

        /// <summary>
        ///     Create a new instance of <see cref="IAsyncDatabaseCommands" /> that will interacts
        ///     with the default database
        /// </summary>
        IAsyncDatabaseCommands ForSystemDatabase();

        /// <summary>
        ///     Force the database commands to read directly from the master, unless there has been a failover.
        /// </summary>
        IDisposable ForceReadFromMaster();

        /// <summary>
        ///     Retrieve a single document for a specified key.
        /// </summary>
        /// <param name="key">key of the document you want to retrieve</param>
        /// <param name="token">The cancellation token.</param>
        Task<JsonDocument> GetAsync(string key, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Retrieves documents with the specified ids, optionally specifying includes to fetch along and also optionally the
        ///     transformer.
        ///     <para>Returns MultiLoadResult where:</para>
        ///     <para>- Results - list of documents in exact same order as in keys parameter</para>
        ///     <para>- Includes - list of documents that were found in specified paths that were passed in includes parameter</para>
        /// </summary>
        /// <param name="keys">array of keys of the documents you want to retrieve</param>
        /// <param name="includes">array of paths in documents in which server should look for a 'referenced' document</param>
        /// <param name="transformer">name of a transformer that should be used to transform the results</param>
        /// <param name="transformerParameters">parameters that will be passed to transformer</param>
        /// <param name="metadataOnly">specifies if only document metadata should be returned</param>
        /// <param name="token">The cancellation token.</param>
        Task<MultiLoadResult> GetAsync(string[] keys, string[] includes, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, bool metadataOnly = false, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Downloads a single attachment.
        /// </summary>
        /// <param name="key">key of the attachment you want to download</param>
        /// <param name="token">The cancellation token.</param>
        [Obsolete("Use RavenFS instead.")]
        Task<Attachment> GetAttachmentAsync(string key, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Downloads attachment metadata for a multiple attachments.
        /// </summary>
        /// <param name="idPrefix">prefix for which attachments should be returned</param>
        /// <param name="start">number of attachments that should be skipped</param>
        /// <param name="pageSize">maximum number of attachments that will be returned</param>
        /// <param name="token">The cancellation token.</param>
        [Obsolete("Use RavenFS instead.")]
        Task<IAsyncEnumerator<Attachment>> GetAttachmentHeadersStartingWithAsync(string idPrefix, int start, int pageSize, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Used to download attachment information for multiple attachments.
        /// </summary>
        /// <param name="start">indicates how many attachments should be skipped</param>
        /// <param name="startEtag">ETag from which to start</param>
        /// <param name="pageSize">maximum number of attachments that will be downloaded</param>
        /// <param name="token">The cancellation token.</param>
        [Obsolete("Use RavenFS instead.")]
        Task<AttachmentInformation[]> GetAttachmentsAsync(int start, Etag startEtag, int pageSize, CancellationToken token = default (CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize, bool metadataOnly = false, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Retrieves multiple documents.
        /// </summary>
        /// <param name="fromEtag">Etag from which documents should start</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="metadataOnly">specifies if only document metadata should be returned</param>
        /// <remarks>
        ///     This is primarily useful for administration of a database
        /// </remarks>
        /// <param name="token">The cancellation token.</param>
        Task<JsonDocument[]> GetDocumentsAsync(Etag fromEtag, int pageSize, bool metadataOnly = false, CancellationToken token = default (CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc, int start = 0, int? pageSize = null, CancellationToken token = default (CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, List<Facet> facets, int start = 0, int? pageSize = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Retrieves an index definition from a database.
        /// </summary>
        /// <param name="name">name of an index</param>
        /// <param name="token">The cancellation token.</param>
        Task<IndexDefinition> GetIndexAsync(string name, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Retrieves indexing performance statistics for all indexes
        /// </summary>
        Task<IndexingPerformanceStatistics[]> GetIndexingPerformanceStatisticsAsync();

        /// <summary>
        ///     Retrieves all suggestions for an index merging
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        Task<IndexMergeResults> GetIndexMergeSuggestionsAsync(CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Retrieves multiple index names from a database.
        /// </summary>
        /// <param name="start">number of index names that should be skipped</param>
        /// <param name="pageSize">maximum number of index names that will be retrieved</param>
        /// <param name="token">The cancellation token.</param>
        Task<string[]> GetIndexNamesAsync(int start, int pageSize, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Retrieves multiple index definitions from a database
        /// </summary>
        /// <param name="start">number of indexes that should be skipped</param>
        /// <param name="pageSize">maximum number of indexes that will be retrieved</param>
        /// <param name="token">The cancellation token.</param>
        Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Gets the license status
        /// </summary>
        Task<LicensingStatus> GetLicenseStatusAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Gets the Logs
        /// </summary>
        Task<LogItem[]> GetLogsAsync(bool errorsOnly, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Sends a multiple faceted queries in a single request and calculates the facet results for each of them
        /// </summary>
        /// <param name="facetedQueries">List of the faceted queries that will be executed on the server-side</param>
        /// <param name="token">The cancellation token.</param>
        Task<FacetResults[]> GetMultiFacetsAsync(FacetQuery[] facetedQueries, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Retrieve the statistics for the database
        /// </summary>
        Task<DatabaseStatistics> GetStatisticsAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Get the all terms stored in the index for the specified field
        ///     You can page through the results by use fromValue parameter as the
        ///     starting point for the next query
        /// </summary>
        /// <param name="index">name of an index</param>
        /// <param name="field">index field</param>
        /// <param name="fromValue">starting point for a query, used for paging</param>
        /// <param name="pageSize">maximum number of terms that will be returned</param>
        /// <param name="token">The cancellation token.</param>
        Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Gets the transformer definition for the specified name
        /// </summary>
        /// <param name="name">transformer name</param>
        /// <param name="token">The cancellation token.</param>
        Task<TransformerDefinition> GetTransformerAsync(string name, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Gets the transformers from the server
        /// </summary>
        /// <param name="start">number of transformers that should be skipped</param>
        /// <param name="pageSize">maximum number of transformers that will be retrieved</param>
        /// <param name="token">The cancellation token.</param>
        Task<TransformerDefinition[]> GetTransformersAsync(int start, int pageSize, CancellationToken token = default(CancellationToken));

        /// <summary>
        /// Sets the transformer's lock mode
        /// </summary>
        /// <param name="name">The name of the transformer</param>
        /// <param name="lockMode">The lock mode to be set</param>
        /// <param name="token">The cancellation token.</param>
        Task SetTransformerLockAsync(string name, TransformerLockMode lockMode, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Retrieves the document metadata for the specified document key.
        ///     <para>Returns:</para>
        ///     <para>The document metadata for the specified document, or <c>null</c> if the document does not exist</para>
        /// </summary>
        /// <param name="key">key of a document to get metadata for</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>The document metadata for the specified document, or null if the document does not exist</returns>
        Task<JsonDocumentMetadata> HeadAsync(string key, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Download attachment metadata for a single attachment.
        /// </summary>
        /// <param name="key">key of the attachment you want to download metadata for</param>
        /// <param name="token">The cancellation token.</param>
        [Obsolete("Use RavenFS instead.")]
        Task<Attachment> HeadAttachmentAsync(string key, CancellationToken token = default(CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<bool> IndexHasChangedAsync(string name, IndexDefinition indexDef, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Return a list of documents that based on the MoreLikeThisQuery.
        /// </summary>
        /// <param name="query">more like this query definition that will be executed</param>
        /// <param name="token">The cancellation token.</param>
        Task<MultiLoadResult> MoreLikeThisAsync(MoreLikeThisQuery query, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Perform a single POST request containing multiple nested GET requests
        /// </summary>
        Task<GetResponse[]> MultiGetAsync(GetRequest[] requests, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Generate the next identity value from the server
        /// </summary>
        Task<long> NextIdentityForAsync(string name, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Sends a patch request for a specific document, ignoring the document's Etag and if the document is missing
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patches">Array of patch requests</param>
        /// <param name="token">The cancellation token.</param>
        Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Sends a patch request for a specific document, ignoring the document's Etag
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patches">Array of patch requests</param>
        /// <param name="ignoreMissing">
        ///     true if the patch request should ignore a missing document, false to throw
        ///     DocumentDoesNotExistException
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, bool ignoreMissing, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Sends a patch request for a specific document, ignoring the document's Etag and  if the document is missing
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
        /// <param name="token">The cancellation token.</param>
        Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Sends a patch request for a specific document, ignoring the document's Etag
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
        /// <param name="ignoreMissing">
        ///     true if the patch request should ignore a missing document, false to throw
        ///     DocumentDoesNotExistException
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, bool ignoreMissing, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Sends a patch request for a specific document
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patches">Array of patch requests</param>
        /// <param name="etag">Require specific Etag [null to ignore]</param>
        /// <param name="token">The cancellation token.</param>
        Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, Etag etag, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Sends a patch request for a specific document which may or may not currently exist
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
        /// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
        /// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
        /// <param name="token">The cancellation token.</param>
        Task<RavenJObject> PatchAsync(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Sends a patch request for a specific document
        /// </summary>
        /// <param name="key">Id of the document to patch</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
        /// <param name="etag">Require specific Etag [null to ignore]</param>
        /// <param name="token">The cancellation token.</param>
        Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, Etag etag, CancellationToken token = default(CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata, CancellationToken token = default(CancellationToken));

#if !DNXCORE50
        /// <summary>
        ///     Prepares the transaction on the server.
        /// </summary>
        Task PrepareTransactionAsync(string txId, Guid? resourceManagerId = null, byte[] recoveryInformation = null, CancellationToken token = default (CancellationToken));
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
        /// <param name="token">The cancellation token.</param>
        Task<PutResult> PutAsync(string key, Etag etag, RavenJObject document, RavenJObject metadata, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Puts a byte array as attachment with the specified key
        /// </summary>
        /// <param name="key">unique key under which attachment will be stored</param>
        /// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
        /// <param name="data">attachment data</param>
        /// <param name="metadata">attachment metadata</param>
        /// <param name="token">The cancellation token.</param>
        [Obsolete("Use RavenFS instead.")]
        Task PutAttachmentAsync(string key, Etag etag, Stream data, RavenJObject metadata, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Creates an index with the specified name, based on an index definition
        /// </summary>
        /// <param name="name">name of an index</param>
        /// <param name="indexDef">definition of an index</param>
        /// <param name="token">The cancellation token.</param>
        Task<string> PutIndexAsync(string name, IndexDefinition indexDef, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Creates multiple indexes with the specified name, using given index definitions and priorities
        /// </summary>
        /// <param name="indexesToAdd">indexes to add</param>
        /// <param name="token">The cancellation token.</param>
        Task<string[]> PutIndexesAsync(IndexToAdd[] indexesToAdd, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Creates multiple side by side indexes with the specified name, using given index definitions and priorities
        /// </summary>
        /// <param name="indexesToAdd">indexes to add</param>
        /// <param name="minimumEtagBeforeReplace">minimum index etag before replace</param>
        /// <param name="replaceTimeUtc">replace time in utc</param>
        /// <param name="token">The cancellation token.</param>
        Task<string[]> PutSideBySideIndexesAsync(IndexToAdd[] indexesToAdd, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Creates an index with the specified name, based on an index definition
        /// </summary>
        /// <param name="name">name of an index</param>
        /// <param name="indexDef">definition of an index</param>
        /// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
        /// <param name="token">The cancellation token.</param>
        Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite, CancellationToken token = default(CancellationToken));

        Task SetIndexLockAsync(string name, IndexLockMode unLockMode, CancellationToken token = default(CancellationToken));

        Task SetIndexPriorityAsync(string name, IndexingPriority priority, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Creates an index with the specified name, based on an index definition that is created by the supplied
        ///     IndexDefinitionBuilder
        /// </summary>
        /// <typeparam name="TDocument">Type of the document index should work on</typeparam>
        /// <typeparam name="TReduceResult">Type of reduce result</typeparam>
        /// <param name="name">name of an index</param>
        /// <param name="indexDef">definition of an index</param>
        /// <param name="token">The cancellation token.</param>
        Task<string> PutIndexAsync<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Creates an index with the specified name, based on an index definition that is created by the supplied
        ///     IndexDefinitionBuilder
        /// </summary>
        /// <typeparam name="TDocument">Type of the document index should work on</typeparam>
        /// <typeparam name="TReduceResult">Type of reduce result</typeparam>
        /// <param name="name">name of an index</param>
        /// <param name="indexDef">definition of an index</param>
        /// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
        /// <param name="token">The cancellation token.</param>
        Task<string> PutIndexAsync<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Creates a transformer with the specified name, based on an transformer definition
        /// </summary>
        /// <param name="name">name of a transformer</param>
        /// <param name="transformerDefinition">definition of a transformer</param>
        /// <param name="token">The cancellation token.</param>
        Task<string> PutTransformerAsync(string name, TransformerDefinition transformerDefinition, CancellationToken token = default(CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes = null, bool metadataOnly = false, bool indexEntriesOnly = false, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Removes all indexing data from a server for a given index so the indexation can start from scratch for that index.
        /// </summary>
        /// <param name="name">name of an index to reset</param>
        /// <param name="token">The cancellation token.</param>
        Task ResetIndexAsync(string name, CancellationToken token = default(CancellationToken));

#if !DNXCORE50
        /// <summary>
        ///     Rollbacks the specified tx id
        /// </summary>
        /// <param name="txId">transaction identifier</param>
        /// <param name="token">The cancellation token.</param>
        Task RollbackAsync(string txId, CancellationToken token = default(CancellationToken));
#endif

        /// <summary>
        ///     Seeds the next identity value on the server
        /// </summary>
        Task<long> SeedIdentityForAsync(string name, long value, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Seeds the next identities value on the server
        /// </summary>
        /// <param name="identities"></param>
        Task SeedIdentitiesAsync(List<KeyValuePair<string, long>> identities, CancellationToken token = default(CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<JsonDocument[]> StartsWithAsync(string keyPrefix, string matches, int start, int pageSize, RavenPagingInformation pagingInformation = null, bool metadataOnly = false, string exclude = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, string skipAfter = null, CancellationToken token = default(CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<RavenJObject>> StreamDocsAsync(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0, int pageSize = int.MaxValue, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
        ///     of the number of items that might be returned.
        /// </summary>
        /// <param name="index">name of an index to query</param>
        /// <param name="query">query definition containing all information required to query a specified index</param>
        /// <param name="queryHeaderInfo">information about performed query</param>
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<RavenJObject>> StreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Returns a list of suggestions based on the specified suggestion query
        /// </summary>
        /// <param name="index">name of an index to query</param>
        /// <param name="suggestionQuery">
        ///     suggestion query definition containing all information required to query a specified
        ///     index
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Updates attachments metadata only.
        /// </summary>
        /// <param name="key">key under which attachment is stored</param>
        /// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
        /// <param name="metadata">attachment metadata</param>
        /// <param name="token">The cancellation token.</param>
        [Obsolete("Use RavenFS instead.")]
        Task UpdateAttachmentMetadataAsync(string key, Etag etag, RavenJObject metadata, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Perform a set based update using the specified index
        /// </summary>
        /// <param name="indexName">name of an index to perform a query on</param>
        /// <param name="queryToUpdate">query that will be performed</param>
        /// <param name="patch">JavaScript patch that will be executed on query results</param>
        /// <param name="options">various operation options e.g. AllowStale or MaxOpsPerSec</param>
        /// <param name="token">The cancellation token.</param>
        Task<Operation> UpdateByIndexAsync(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, BulkOperationOptions options = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Perform a set based update using the specified index
        /// </summary>
        /// <param name="indexName">name of an index to perform a query on</param>
        /// <param name="queryToUpdate">query that will be performed</param>
        /// <param name="patchRequests">array of patches that will be executed on a query results</param>
        /// <param name="options">various operation options e.g. AllowStale or MaxOpsPerSec</param>
        /// <param name="token">The cancellation token.</param>
        Task<Operation> UpdateByIndexAsync(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, BulkOperationOptions options = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Get the full URL for the given document key
        /// </summary>
        string UrlFor(string documentKey);

        /// <summary>
        ///     Returns a new <see cref="IAsyncDatabaseCommands" /> using the specified credentials
        /// </summary>
        /// <param name="credentialsForSession">The credentials for session.</param>
        IAsyncDatabaseCommands With(ICredentials credentialsForSession);
    }

    public interface IAsyncGlobalAdminDatabaseCommands
    {
        IAsyncDatabaseCommands Commands { get; }

        /// <summary>
        ///     Sends an async command to compact a database. During the compaction the specified database will be offline.
        /// </summary>
        /// <param name="databaseName">name of a database to compact</param>
        /// <param name="token">The cancellation token.</param>
        Task<Operation> CompactDatabaseAsync(string databaseName, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Creates a database
        /// </summary>
        Task CreateDatabaseAsync(DatabaseDocument databaseDocument, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Used to delete a database from a server, with a possibility to remove all the data from hard drive.
        ///     <para>
        ///         Warning: if hardDelete is set to <c>true</c> then ALL data will be removed from the data directory of a
        ///         database.
        ///     </para>
        /// </summary>
        /// <param name="databaseName">name of a database to delete</param>
        /// <param name="hardDelete">should all data be removed (data files, indexing files, etc.). Default: false</param>
        /// <param name="token">The cancellation token.</param>
        Task DeleteDatabaseAsync(string databaseName, bool hardDelete = false, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Ensures that the database exists, creating it if needed
        /// </summary>
        Task EnsureDatabaseExistsAsync(string name, bool ignoreFailures = false, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Gets the build number
        /// </summary>
        Task<BuildNumber> GetBuildNumberAsync(CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Returns the names of all tenant databases on the RavenDB server
        /// </summary>
        Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Gets server-wide statistics.
        /// </summary>
        Task<AdminStatistics> GetStatisticsAsync(CancellationToken token = default (CancellationToken));

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
        /// <param name="token">The cancellation token.</param>
        Task<Operation> StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument, bool incremental, string databaseName, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Begins a restore operation.
        /// </summary>
        Task<Operation> StartRestoreAsync(DatabaseRestoreRequest restoreRequest, CancellationToken token = default (CancellationToken));
    }

    public interface IAsyncAdminDatabaseCommands
    {
        /// <summary>
        ///     Gets configuration for current database.
        /// </summary>
        Task<RavenJObject> GetDatabaseConfigurationAsync(CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Get the indexing status
        /// </summary>
        Task<IndexingStatus> GetIndexingStatusAsync(CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Enables indexing.
        /// </summary>
        /// <param name="maxNumberOfParallelIndexTasks">
        ///     if set then maximum number of parallel indexing tasks will be set to this
        ///     value.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task StartIndexingAsync(int? maxNumberOfParallelIndexTasks = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Disables all indexing.
        /// </summary>
        Task StopIndexingAsync(CancellationToken token = default (CancellationToken));
    }

    public interface IAsyncInfoDatabaseCommands
    {
        /// <summary>
        ///     Get replication info
        /// </summary>
        Task<ReplicationStatistics> GetReplicationInfoAsync(CancellationToken token = default (CancellationToken));
    }
}

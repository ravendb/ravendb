// -----------------------------------------------------------------------
//  <copyright file="ISmugglerDatabaseOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerDatabaseOperations
	{
        SmugglerDatabaseOptions Options { get; }

		Task DeleteDocument(string key);

		Task<Etag> ExportDocumentsDeletion(JsonTextWriter jsonWriter, Etag startDocsEtag, Etag maxEtag);

		LastEtagsInfo FetchCurrentMaxEtags();

		JsonDocument GetDocument(string key);

		Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag, int take);

		Task<RavenJArray> GetIndexes(int totalCount);

		Task<DatabaseStatistics> GetStats();

		Task<RavenJArray> GetTransformers(int start);

		Task<string> GetVersion(RavenConnectionStringOptions server);

		void PurgeTombstones(OperationState result);

		Task PutDocument(RavenJObject document, int size);

		Task PutIndex(string indexName, RavenJToken index);

		Task PutTransformer(string transformerName, RavenJToken transformer);

		void ShowProgress(string format, params object[] args);

		Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript);

		RavenJObject StripReplicationInformationFromMetadata(RavenJObject metadata);

        void Initialize(SmugglerDatabaseOptions options);

        void Configure(SmugglerDatabaseOptions options);

		Task<List<KeyValuePair<string, long>>> GetIdentities();

		Task SeedIdentityFor(string identityName, long identityValue);

		string GetIdentifier();
	}
}
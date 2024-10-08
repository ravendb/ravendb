//-----------------------------------------------------------------------
// <copyright file="RevisionsConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    /// <summary>
    /// Operation to retrieve revisions of a document in the RavenDB database.
    /// Provides the ability to specify pagination through start index and page size.
    /// </summary>
    /// <typeparam name="T">The type of the document for which the revisions are being retrieved.</typeparam>
    public sealed class GetRevisionsOperation<T> : IOperation<RevisionsResult<T>>
    {
        private readonly Parameters _parameters;

        /// <summary>
        /// Operation to retrieve revisions of a document in the RavenDB database.
        /// Provides the ability to specify pagination through start index and page size.
        /// Initializes a new instance of the <see cref="GetRevisionsOperation{T}"/> class for the specified document ID.
        /// </summary>
        /// <typeparam name="T">The type of the document for which the revisions are being retrieved.</typeparam>
        /// <param name="id">The ID of the document for which revisions are being retrieved.</param>
        public GetRevisionsOperation(string id)
            : this(new Parameters { Id = id })
        {
        }

        /// <summary>
        /// Operation to retrieve revisions of a document in the RavenDB database.
        /// Provides the ability to specify pagination through start index and page size.
        /// Initializes a new instance of the <see cref="GetRevisionsOperation{T}"/> class for the specified document ID,
        /// with pagination parameters to retrieve a specific subset of revisions.
        /// </summary>
        /// <typeparam name="T">The type of the document for which the revisions are being retrieved.</typeparam>
        /// <param name="id">The ID of the document for which revisions are being retrieved.</param>
        /// <param name="start">The starting index of the revisions to be retrieved.</param>
        /// <param name="pageSize">The number of revisions to retrieve.</param>
        public GetRevisionsOperation(string id, int start, int pageSize)
            : this(new Parameters { Id = id, Start = start, PageSize = pageSize })
        {
        }

        /// <summary>
        /// Operation to retrieve revisions of a document in the RavenDB database.
        /// Provides the ability to specify pagination through start index and page size.
        /// Initializes a new instance of the <see cref="GetRevisionsOperation{T}"/> class with the specified parameters.
        /// </summary>
        /// <typeparam name="T">The type of the document for which the revisions are being retrieved.</typeparam>
        /// <param name="parameters">The parameters specifying the document ID and optional pagination settings.</param>
        /// <exception cref="ArgumentNullException">Thrown when the <paramref name="parameters"/> are null or invalid.</exception>
        public GetRevisionsOperation(Parameters parameters)
        {
            if (parameters is null)
                throw new ArgumentNullException(nameof(parameters));

            parameters.Validate();

            _parameters = parameters;
        }

        public RavenCommand<RevisionsResult<T>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetRevisionsResultCommand(_parameters.Id, _parameters.Start, _parameters.PageSize, serialization: store.Conventions.Serialization);
        }

        /// <summary>
        /// Parameters for the <see cref="GetRevisionsOperation{T}"/> class, specifying the document ID
        /// and optional pagination settings.
        /// </summary>
        public sealed class Parameters
        {
            /// <summary>
            /// Gets or sets the ID of the document for which revisions are being retrieved.
            /// </summary>
            public string Id { get; set; }

            /// <summary>
            /// Gets or sets the starting index of the revisions to be retrieved.
            /// If <c>null</c>, all revisions will be retrieved from the beginning.
            /// </summary>
            public int? Start { get; set; }

            /// <summary>
            /// Gets or sets the number of revisions to retrieve. If <c>null</c>, all revisions from the starting index will be retrieved.
            /// </summary>
            public int? PageSize { get; set; }

            internal void Validate()
            {
                if (string.IsNullOrEmpty(Id))
                    throw new ArgumentNullException(nameof(Id));
            }
        }

        private sealed class GetRevisionsResultCommand : RavenCommand<RevisionsResult<T>>
        {
            public override bool IsReadRequest => true;

            private readonly ISerializationConventions _serialization;

            private readonly GetRevisionsCommand _cmd;

            public GetRevisionsResultCommand(string id, int? start, int? pageSize, ISerializationConventions serialization)
            {
                _serialization = serialization;
                _cmd = new GetRevisionsCommand(id, start, pageSize);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                return _cmd.CreateRequest(ctx, node, out url);
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null || response.TryGet(nameof(RevisionsResult<T>.Results), out BlittableJsonReaderArray revisions) == false)
                    return;

                response.TryGet(nameof(RevisionsResult<T>.TotalResults), out int total);

                var results = new List<T>(revisions.Length);
                foreach (BlittableJsonReaderObject revision in revisions)
                {
                    if (revision == null)
                        continue;

                    var entity = _serialization.DeserializeEntityFromBlittable<T>(revision);
                    results.Add(entity);
                }

                Result = new RevisionsResult<T>
                {
                    Results = results,
                    TotalResults = total
                };
            }
        }
    }
}

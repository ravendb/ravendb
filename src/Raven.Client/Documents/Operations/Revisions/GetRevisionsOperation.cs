//-----------------------------------------------------------------------
// <copyright file="RevisionsConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class GetRevisionsOperation<T> : IOperation<RevisionsResult<T>>
    {
        private readonly GetRevisionsResultCommand _command;

        public GetRevisionsOperation(string id, int? start = 0, int? pageSize = int.MaxValue, bool metadataOnly = false)
        {
            _command = new GetRevisionsResultCommand(id, start, pageSize, metadataOnly);
        }

        public RavenCommand<RevisionsResult<T>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return _command;
        }


        private class GetRevisionsResultCommand : RavenCommand<RevisionsResult<T>>
        {
            public override bool IsReadRequest => true;

            private static readonly BlittableJsonConverter Converter = new BlittableJsonConverter(DocumentConventions.Default.Serialization);

            private readonly GetRevisionsCommand _cmd;

            public GetRevisionsResultCommand(string id, int? start, int? pageSize, bool metadataOnly = false)
            {
                _cmd = new GetRevisionsCommand(id, start, pageSize, metadataOnly);
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
                    
                    revision.TryGetId(out var id);
                    var entity = Converter.FromBlittable<T>(revision, id);
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

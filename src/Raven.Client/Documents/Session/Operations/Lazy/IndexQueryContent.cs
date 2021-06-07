using System;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class IndexQueryContent : GetRequest.IContent
    {
        private readonly DocumentConventions _conventions;
        private readonly IndexQuery _query;

        public IndexQueryContent(DocumentConventions conventions, IndexQuery query)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _query = query ?? throw new ArgumentNullException(nameof(query));
        }

        public void WriteContent(AbstractBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WriteIndexQuery(_conventions, context, _query);
        }
    }
}

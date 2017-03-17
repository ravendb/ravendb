using System.Collections.Generic;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlDocumentTransformer : EtlTransformer<RavenEtlItem, ICommandData>
    {
        private readonly List<ICommandData> _commands = new List<ICommandData>();
        private readonly PatchRequest _transformationScript;

        public RavenEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, RavenEtlConfiguration configuration) : base(database, context)
        {
            _transformationScript = new PatchRequest { Script = configuration.Script };
        }

        public override IEnumerable<ICommandData> GetTransformedResults()
        {
            return _commands;
        }

        public override void Transform(RavenEtlItem item)
        {
            if (item.IsDelete)
                _commands.Add(new DeleteCommandData(item.DocumentKey, null));
            else
            {
                var result = Apply(Context, item.Document, _transformationScript);

                _commands.Add(new PutCommandDataWithBlittableJson(item.DocumentKey, null, result.ModifiedDocument));
            }
        }
    }
}
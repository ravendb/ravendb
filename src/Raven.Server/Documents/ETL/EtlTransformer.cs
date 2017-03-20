using System.Collections.Generic;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlTransformer<TExtracted, TTransformed> : PatchDocument
    {
        protected readonly DocumentsOperationContext Context;

        protected EtlTransformer(DocumentDatabase database, DocumentsOperationContext context) : base(database)
        {
            Context = context;
        }

        public abstract IEnumerable<TTransformed> GetTransformedResults();

        public abstract void Transform(TExtracted item);
    }
}
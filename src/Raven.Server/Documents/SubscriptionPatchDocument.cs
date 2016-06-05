using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Runtime;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents
{
    public class SubscriptionPatchDocument:PatchDocument
    {
        public SubscriptionPatchDocument(DocumentDatabase database) : base(database)
        {
        }
        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            
        }
        public bool MatchCriteria(DocumentsOperationContext context, Document document, PatchRequest patch)
        {
            var actualPatchResult = ApplySingleScript(context, document, false, patch).ActualPatchResult;
            return actualPatchResult.AsBoolean();
        }
    }
}

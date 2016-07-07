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
        private readonly PatchRequest _patchRequest;

        public SubscriptionPatchDocument(DocumentDatabase database, string filterJavaScript) : base(database)
        {
            _patchRequest = new PatchRequest
            {
                Script =  filterJavaScript
            };
            
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            // override to make it "no-op"
        }
        public bool MatchCriteria(DocumentsOperationContext context, Document document)
        {
            var actualPatchResult = ApplySingleScript(context, document, false, _patchRequest).ActualPatchResult;
            return actualPatchResult.AsBoolean();
        }
    }
}

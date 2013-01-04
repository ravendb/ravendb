using System.Collections.Generic;
using Raven.Studio.Features.Documents;

namespace Raven.Studio.Models
{
    public class DocumentViewStateStore
    {
        public Dictionary<string, ColumnsModel> documentViewState = new Dictionary<string, ColumnsModel>();
 
        public void SetDocumentState(string context, ColumnsModel columns)
        {
            documentViewState[context] = columns;
        }

        public ColumnsModel GetDocumentState(string context)
        {
            ColumnsModel model;
            return documentViewState.TryGetValue(context, out model) ? model : null;
        }
    }
}
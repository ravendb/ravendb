using System;
using System.Collections.Generic;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
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

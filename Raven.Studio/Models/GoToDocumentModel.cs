using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class GoToDocumentModel : ViewModel
    {
        private string documentId = string.Empty;
        private ICommand goToDocument;

        public string DocumentId
        {
            get { return documentId; }
            set
            {
                documentId = value;
                OnPropertyChanged(() => DocumentId);
            }
        }

        public ICommand GoToDocument
        {
            get { return goToDocument ?? (goToDocument = new ActionCommand(HandleGoToDocument)); }
        }

        private void HandleGoToDocument()
        {
            if (!string.IsNullOrEmpty(DocumentId))
            {
                UrlUtil.Navigate("/edit?id=" + DocumentId);
            }
        }
    }
}

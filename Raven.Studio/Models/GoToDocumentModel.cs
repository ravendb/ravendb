using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
    public class GoToDocumentModel : ViewModel, IAutoCompleteSuggestionProvider
    {
        private string documentId = string.Empty;
        private ICommand goToDocument;

        public GoToDocumentModel()
        {
        }

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

        public Task<IList<object>> ProvideSuggestions(string enteredText)
        {
            return ApplicationModel.Database.Value.AsyncDatabaseCommands.GetDocumentsStartingWithAsync(DocumentId, 0, 50)
                .ContinueWith(t => (IList<object>)t.Result.Select(d => d.Key).Cast<object>().ToList());
        }
    }
}

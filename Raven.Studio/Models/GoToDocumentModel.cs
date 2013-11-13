using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;

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
            return ApplicationModel.Database.Value.AsyncDatabaseCommands.StartsWithAsync(DocumentId, null, 0, 25, metadataOnly: true)
                .ContinueWith(t => (IList<object>)t.Result.Select(d => d.Key).Cast<object>().ToList());
        }
    }
}

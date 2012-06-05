using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
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
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
    public class GoToDocumentModel : ViewModel
    {
        private string documentId = string.Empty;
        private ICommand goToDocument;
        private List<string> previousDocuments = new List<string>();

        public GoToDocumentModel()
        {
            Suggestions = new ObservableCollection<string>();
        }

        public string DocumentId
        {
            get { return documentId; }
            set
            {
                documentId = value;
                BeginSuggestionsUpdate();
                OnPropertyChanged(() => DocumentId);
            }
        }

        private void BeginSuggestionsUpdate()
        {
            if (DocumentId.Length > 3)
            {
                ApplicationModel.Database.Value.AsyncDatabaseCommands.GetDocumentsStartingWithAsync(DocumentId, 0, 50)
                    .ContinueOnSuccessInTheUIThread(documents =>
                                                        {
                                                            Suggestions.Clear();
                                                            Suggestions.AddRange(documents.Select(d => d.Key));
                                                            Suggestions.AddRange(previousDocuments);
                                                        });
            }
            else
            {
                Suggestions.Clear();
                Suggestions.AddRange(previousDocuments);
            }
        }

        public ObservableCollection<string> Suggestions { get; private set; }

        public ICommand GoToDocument
        {
            get { return goToDocument ?? (goToDocument = new ActionCommand(HandleGoToDocument)); }
        }

        private void HandleGoToDocument()
        {
            if (!string.IsNullOrEmpty(DocumentId))
            {
                UrlUtil.Navigate("/edit?id=" + DocumentId);
                previousDocuments.Add(DocumentId);
            }
        }
    }
}

using System;
using System.Collections.Generic;
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
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Behaviors;
using Raven.Studio.Features.JsonEditor;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Studio.Extensions;

namespace Raven.Studio.Features.Documents
{
    public class DocumentPadModel : Model
    {
        private bool isOpen;
        private string documentId;
        private DocumentSuggestor _suggestor;
        private ICommand loadDocumentCommand;
        private static JsonSyntaxLanguageExtended JsonLanguage;
        private EditorDocument document;
        private string statusMessage;
        private bool isDocumentLoaded;
        private ICommand closeCommand;

        static DocumentPadModel()
        {
            JsonLanguage = new JsonSyntaxLanguageExtended();   
        }

        public DocumentPadModel()
        {
            document = new EditorDocument() {Language = JsonLanguage};
        }

        public bool IsOpen
        {
            get { return isOpen; }
            set
            {
                isOpen = value;
                OnPropertyChanged(() => IsOpen);
            }
        }

        public string StatusMessage
        {
            get { return statusMessage; }
            private set
            {
                statusMessage = value;
                OnPropertyChanged(() => StatusMessage);
            }
        }

        public bool IsDocumentLoaded
        {
            get { return isDocumentLoaded; }
            private set
            {
                isDocumentLoaded = value;
                OnPropertyChanged(() => IsDocumentLoaded);
            }
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

        public ICommand LoadDocument
        {
            get 
            { return loadDocumentCommand ?? (loadDocumentCommand = new AsyncActionCommand(HandleLoadDocument)); }
        }

        public ICommand Close
        {
            get { return closeCommand ?? (closeCommand = new ActionCommand(() => IsOpen = false)); }
        }

        private async Task HandleLoadDocument()
        {
            try
            {
                IsDocumentLoaded = false;
                StatusMessage = "Loading document ...";

                var doc = await ApplicationModel.DatabaseCommands.GetAsync(DocumentId);

                Document.SetText(doc.DataAsJson.ToString());
                Document.OutliningMode = OutliningMode.None;
                Document.OutliningMode = OutliningMode.Automatic;
                Document.OutliningManager.EnsureCollapsed();

                IsDocumentLoaded = true;
            }
            catch (Exception ex)
            {
                StatusMessage = "Failed to load document";
                IsDocumentLoaded = false;
            }
        }

        public IAutoCompleteSuggestionProvider DocumentIdSuggestions
        {
            get { return _suggestor ?? (_suggestor = new DocumentSuggestor(this)); }
        }
        public EditorDocument Document
        {
            get { return document; }
        }

        private class DocumentSuggestor : IAutoCompleteSuggestionProvider
        {
            private DocumentPadModel model;

            public DocumentSuggestor(DocumentPadModel documentPadModel)
            {
                model = documentPadModel;
            }

            public async Task<IList<object>> ProvideSuggestions(string enteredText)
            {
                try
                {
                    var results =
                        await ApplicationModel.Database.Value.AsyncDatabaseCommands.StartsWithAsync(enteredText, 0, 25,
                                                                                                    metadataOnly: true);
                    return results.Select(d => d.Key).Cast<object>().ToList();
                }
                catch
                {
                    return new object[0];
                }
            }
        }
    }
}

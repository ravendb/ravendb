using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Extensions;
using Raven.Studio.Models;

namespace Raven.Studio.Features.JsonEditor
{
    public partial class QuickDocumentView : UserControl
    {
        public string DocumentId
        {
            get { return (string)GetValue(DocumentIdProperty); }
            set { SetValue(DocumentIdProperty, value); }
        }

        public event EventHandler<EventArgs> DocumentShown;

        protected virtual void OnDocumentShown()
        {
            var handler = DocumentShown;
            if (handler != null) handler(this, EventArgs.Empty);
        }

        public static readonly DependencyProperty DocumentIdProperty =
            DependencyProperty.Register("DocumentId", typeof(string), typeof(QuickDocumentView), new PropertyMetadata(""));

        private ICommand _showDocumentCommand;

        private bool _documentLoaded;
        private ICommand _openInDocumentPadCommand;

        public QuickDocumentView()
        {
            InitializeComponent();

            Unloaded += HandleUnloaded;
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            Editor.IntelliPrompt.CloseAllSessions();
        }

        public ICommand ShowDocument
        {
            get { return _showDocumentCommand ?? (_showDocumentCommand = new ActionCommand(HandleShowDocument)); }
        }

        public ICommand OpenInDocumentPad
        {
            get { return _openInDocumentPadCommand ?? (_openInDocumentPadCommand = new ActionCommand(HandleOpenInDocumentPad)); }
        }

        private void HandleOpenInDocumentPad()
        {
            ApplicationModel.Current.ShowDocumentInDocumentPad(DocumentId);
            Editor.IntelliPrompt.CloseAllSessions();
        }

        private async void HandleShowDocument()
        {
            if (_documentLoaded)
            {
                return;
            }

            EditorGrid.Visibility = Visibility.Visible;

            try
            {
                StatusMessage.Text = "Loading document ...";
                StatusMessage.Visibility = Visibility.Visible;

                var doc = await ApplicationModel.DatabaseCommands.GetAsync(DocumentId);
                Editor.Document.SetText(doc.DataAsJson.ToString());
                Editor.Document.IsReadOnly = true;

                // since we're in a compact environment, collapse the document automatically
                Editor.Document.OutliningMode = OutliningMode.None;
                Editor.Document.OutliningMode = OutliningMode.Automatic;
                Editor.Document.OutliningManager.EnsureCollapsed();

                _documentLoaded = true;
                StatusMessage.Visibility = Visibility.Collapsed;

                OnDocumentShown();
            }
            catch (Exception ex)
            {
                StatusMessage.Text = "Failed to load document";
                StatusMessage.Visibility = Visibility.Visible;
            }
        }

    }
}

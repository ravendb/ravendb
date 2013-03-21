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

        public static readonly DependencyProperty DocumentIdProperty =
            DependencyProperty.Register("DocumentId", typeof(string), typeof(QuickDocumentView), new PropertyMetadata(""));

        
        public QuickDocumentView()
        {
            InitializeComponent();
        }

        private async void ButtonBase_OnClick(object sender, RoutedEventArgs e)
        {
            try
            {
                var doc = await ApplicationModel.DatabaseCommands.GetAsync(DocumentId);
                Editor.Document.SetText(doc.DataAsJson.ToString());
                Editor.Document.IsReadOnly = true;

                Editor.Visibility = Visibility.Visible;
            }
            catch (Exception ex)
            {
                
            }
        }
    }
}

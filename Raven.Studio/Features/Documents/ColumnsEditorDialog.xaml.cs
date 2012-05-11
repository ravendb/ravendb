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

namespace Raven.Studio.Features.Documents
{
    public partial class ColumnsEditorDialog : ChildWindow
    {
        public ColumnsEditorDialog()
        {
            InitializeComponent();
        }

        public static void Show(ColumnsModel columns, string context)
        {
            var dialog = new ColumnsEditorDialog()
                             {
                                 DataContext = new ColumnsEditorDialogViewModel(columns, context)
                             };
            dialog.Show();
        }

        private void OKButton_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = true;
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = false;
        }
    }
}


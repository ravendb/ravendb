using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public partial class ColumnsEditorDialog : DialogView
    {
        public ColumnsEditorDialog()
        {
            InitializeComponent();

        }

        public static void Show(ColumnsModel columns, string context, Func<Task<IList<SuggestedColumn>>> documentSampler)
        {
            var dialog = new ColumnsEditorDialog()
                             {
                                 DataContext = new ColumnsEditorDialogViewModel(columns, context, documentSampler)
                             };
            dialog.Show();
        }
    }
}


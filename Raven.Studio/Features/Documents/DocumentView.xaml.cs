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
    public partial class DocumentView : UserControl
    {
        private const double ExpandedMinimumHeight = 110;

        public DocumentView()
        {
            InitializeComponent();

            SizeChanged += HandleSizeChanged;

            VisualStateManager.GoToState(this, "Collapsed", false);
        }

        private void HandleSizeChanged(object sender, SizeChangedEventArgs e)
        {
            if (e.NewSize.Height >= ExpandedMinimumHeight && e.PreviousSize.Height < ExpandedMinimumHeight)
            {
                VisualStateManager.GoToState(this, "Expanded", true);
            }

            if (e.NewSize.Height < ExpandedMinimumHeight && e.PreviousSize.Height >= ExpandedMinimumHeight)
            {
                VisualStateManager.GoToState(this, "Collapsed", true);
            }
        }
    }
}

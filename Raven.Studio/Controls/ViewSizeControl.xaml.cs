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

namespace Raven.Studio.Controls
{
    public partial class ViewSizeControl : UserControl
    {
        public static readonly DependencyProperty ViewSizeProperty =
            DependencyProperty.Register("ViewSize", typeof (double), typeof (ViewSizeControl), new PropertyMetadata(default(double)));

        public double ViewSize
        {
            get { return (double) GetValue(ViewSizeProperty); }
            set { SetValue(ViewSizeProperty, value); }
        }

        public ViewSizeControl()
        {
            InitializeComponent();
        }
    }

}

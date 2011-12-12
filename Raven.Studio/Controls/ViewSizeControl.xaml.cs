using System.Windows;
using System.Windows.Controls;

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
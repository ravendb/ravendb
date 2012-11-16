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
    public partial class ProgressWindow : ChildWindow
    {
        public ProgressWindow()
        {
            InitializeComponent();
        }

        public static readonly DependencyProperty TitleProperty =
            DependencyProperty.Register("Title", typeof (string), typeof (ProgressWindow), new PropertyMetadata(default(string)));

        public static readonly DependencyProperty ProgressProperty =
            DependencyProperty.Register("Progress", typeof (int), typeof (ProgressWindow), new PropertyMetadata(default(int)));

        public int Progress
        {
            get { return (int) GetValue(ProgressProperty); }
            set { SetValue(ProgressProperty, value); }
        }

        public string Title
        {
            get { return (string) GetValue(TitleProperty); }
            set { SetValue(TitleProperty, value); }
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


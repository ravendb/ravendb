using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public class ColumnModel : ViewModel
    {
        private string header;
        private string binding;
        private string defaultWidth;

        public string Header
        {
            get { return header; }
            set
            {
                if (header != value)
                {
                    header = value;
                    OnPropertyChanged(() => Header);
                }
            }
        }

        public string Binding
        {
            get { return binding; }
            set
            {
                if (binding != value)
                {
                    binding = value;
                    OnPropertyChanged(() => Binding);
                }
            }
        }

        public string DefaultWidth
        {
            get { return defaultWidth; }
            set
            {
                if (defaultWidth != value)
                {
                    defaultWidth = value;
                    OnPropertyChanged(() => DefaultWidth);
                }
            }
        }
    }
}

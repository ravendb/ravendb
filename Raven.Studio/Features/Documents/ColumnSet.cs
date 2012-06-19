using System;
using System.Collections.Generic;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Features.Documents
{
    public class ColumnSet
    {
        private IList<ColumnDefinition> columns;

        public IList<ColumnDefinition> Columns
        {
            get { return columns ?? (columns = new List<ColumnDefinition>()); }
            set { columns = value; }
        }
    }
}

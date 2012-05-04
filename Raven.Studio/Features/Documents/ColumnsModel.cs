using System;
using System.Collections.ObjectModel;
using System.Linq;
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
    public class ColumnsModel : Model
    {
        public ObservableCollection<ColumnModel> Columns { get; private set; }
 
        public ColumnsModel()
        {
            Columns = new ObservableCollection<ColumnModel>();
        }

        public void LoadFromColumnSet(ColumnSet columnSet)
        {
            Columns.Clear();

            foreach (var columnModel in columnSet.Columns.Select(ToColumnModel))
            {
                Columns.Add(columnModel);
            }
        }

        private ColumnModel ToColumnModel(ColumnDefinition column)
        {
            return new ColumnModel()
                       {
                           Binding = column.Binding, 
                           Header = column.Header,
                           DefaultWidth = column.DefaultWidth,
                       };
        }
    }
}

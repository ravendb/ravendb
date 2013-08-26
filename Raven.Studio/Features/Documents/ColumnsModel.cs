using System.Collections.Generic;
using System.Collections.ObjectModel;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public enum ColumnsSource
    {
        Automatic,
		Resize,
        User
    }

    public class ColumnsModel : Model
    {
        public ObservableCollection<ColumnDefinition> Columns { get; private set; }
 
        public ColumnsModel()
        {
            Columns = new ObservableCollection<ColumnDefinition>();
        }

        public ColumnsSource Source { get; set; }

        public void LoadFromColumnDefinitions(IEnumerable<ColumnDefinition> columnDefinitions)
        {
            Columns.Clear();

            foreach (var column in columnDefinitions)
            {
                Columns.Add(column);
            }
        }
    }
}
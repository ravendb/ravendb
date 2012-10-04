using System.Collections.Generic;

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
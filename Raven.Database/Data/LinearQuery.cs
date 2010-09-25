using System;

namespace Raven.Database.Data
{
    [Serializable]
    public class LinearQuery
    {
        public string Query { get; set; }
        public int Start { get; set; }
        public int PageSize { get; set; }

        public LinearQuery()
        {
            PageSize = 128;
        }
    }
}
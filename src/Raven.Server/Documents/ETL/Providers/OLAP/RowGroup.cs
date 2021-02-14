using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class RowGroup
    {
        public RowGroup()
        {
            Data = new Dictionary<string, IList>();
            Ids = new List<string>();
        }

        public Dictionary<string, IList> Data { get; set; }

        public List<string> Ids { get; }

        public int Count { get; internal set; }
    }
}

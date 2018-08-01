using System.Collections.Generic;

namespace Raven.Client.Documents.Graph
{
    public class EdgeInfo
    {        
        public string To { get; set; }
        public Dictionary<string, string> Attributes { get; } = new Dictionary<string, string>();
    }
}

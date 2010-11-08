namespace Raven.Client.Silverlight.Data
{
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;

    public class Index : Entity
    {
        public Index()
        {
            this.Fields = new List<IndexField>();
        }

        public string Name { get; set; }

        public string Map { get; set; }

        public string Reduce { get; set; }

        public IList<IndexField> Fields { get; private set; }

        public override JObject ToJson()
        {
            var index = new JObject();

            if(string.IsNullOrEmpty(this.Map))
            {
                this.Map = string.Empty;
            }

            index["Map"] = this.Map;

            return index;
        }
    }
}

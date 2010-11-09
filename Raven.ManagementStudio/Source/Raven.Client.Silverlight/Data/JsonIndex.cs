namespace Raven.Client.Silverlight.Data
{
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;

    public class JsonIndex : Entity
    {
        public JsonIndex()
        {
            this.Fields = new List<JsonIndexField>();
        }

        public string Name { get; set; }

        public string Map { get; set; }

        public string Reduce { get; set; }

        public IList<JsonIndexField> Fields { get; private set; }

        public override JObject ToJson()
        {
            var index = new JObject();

            if (string.IsNullOrEmpty(this.Map))
            {
                this.Map = string.Empty;
            }

            index["Map"] = this.Map;

            return index;
        }
    }
}

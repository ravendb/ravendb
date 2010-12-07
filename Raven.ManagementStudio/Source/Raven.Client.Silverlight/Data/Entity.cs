namespace Raven.Client.Silverlight.Data
{
    using Newtonsoft.Json.Linq;

    public abstract class Entity
    {
        public string Id { get; set; }

        public abstract JObject ToJson();
    }
}

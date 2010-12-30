namespace Raven.Client.Silverlight.Common.Mappers
{
    using Newtonsoft.Json.Linq;
    using Raven.Client.Silverlight.Data;

    public class IndexMapper : IMapper<JsonIndex>
    {
        public JsonIndex Map(string json)
        {
            var index = new JsonIndex();

            if (!string.IsNullOrEmpty(json))
            {
                JObject jObject = JObject.Parse(json);

                if (jObject["name"] != null)
                {
                    index.Name = jObject["name"].ToString().Replace("\"", string.Empty);
                    index.Id = index.Name;
                }

                if (jObject["definition"] != null)
                {
                    index.Map = jObject["definition"]["Map"].ToString().Replace("\"", string.Empty);
                    index.Reduce = jObject["definition"]["Reduce"].ToString().Replace("\"", string.Empty);
                }
            }

            return index;
        }
    }
}

namespace Raven.Management.Client.Silverlight.Common.Mappers
{
    using System.Collections.Generic;
    using System.IO;
    using Database.Indexing;
    using Document;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// 
    /// </summary>
    public class IndexMapper : IMapper<KeyValuePair<string, IndexDefinition>>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="convention"></param>
        public IndexMapper(DocumentConvention convention)
        {
            Convention = convention;
        }

        private DocumentConvention Convention { get; set; }

        #region IMapper<KeyValuePair<string,IndexDefinition>> Members

        /// <summary>
        /// 
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public KeyValuePair<string, IndexDefinition> Map(string json)
        {
            JObject jObject = JObject.Parse(json);
            var serializer = new JsonSerializer();

            var index = (IndexDefinition) serializer.Deserialize(jObject["definition"].CreateReader(), typeof (IndexDefinition));
            var name = jObject.Value<string>("name");

            return new KeyValuePair<string, IndexDefinition>(name, index);
        }

        #endregion
    }
}
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
            JObject jsonIndex = JObject.Load(new JsonTextReader(new StringReader(json)));

            string name = jsonIndex["Name"].ToString();
            var index = Convention.CreateSerializer().Deserialize<IndexDefinition>(new JTokenReader(jsonIndex["Index"]));

            return new KeyValuePair<string, IndexDefinition>(name, index);
        }

        #endregion
    }
}
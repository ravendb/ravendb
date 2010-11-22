namespace Raven.Management.Client.Silverlight.Common.Mappers
{
    using System;
    using Raven.Database.Data;
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json;
    using System.Collections.Generic;

    public class QueryResultMapper : IMapper<QueryResult>
    {
        #region IMapper<QueryResult> Members

        public QueryResult Map(string json)
        {
            var jObject = JObject.Parse(json);
            var jsonSerializer = new JsonSerializer();

            return (QueryResult)jsonSerializer.Deserialize(jObject.CreateReader(), typeof(QueryResult));
        }

        #endregion
    }
}
namespace Raven.Management.Client.Silverlight.Common.Mappers
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Raven.Database.Data;

    public class StatisticsMapper : IMapper<DatabaseStatistics>
    {
        #region IMapper<DatabaseStatistics> Members

        public DatabaseStatistics Map(string json)
        {
            JObject jObject = JObject.Parse(json);
            var jsonSerializer = new JsonSerializer();

            return (DatabaseStatistics) jsonSerializer.Deserialize(jObject.CreateReader(), typeof (DatabaseStatistics));
        }

        #endregion
    }
}
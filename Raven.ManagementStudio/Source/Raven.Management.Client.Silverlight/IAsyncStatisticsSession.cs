namespace Raven.Management.Client.Silverlight
{
    using Raven.Database.Data;
    using Raven.Management.Client.Silverlight.Common;

    public interface IAsyncStatisticsSession
    {
        void Load(CallbackFunction.Load<DatabaseStatistics> callback);
    }
}
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class HomeModelLoactor : ModelLocatorBase<HomeModel>
    {
        protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<HomeModel> observable)
        {
            observable.Value = new HomeModel(asyncDatabaseCommands);
        }
    }
}
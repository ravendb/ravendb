using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Raven.Server.Utils.Features;
using Sparrow.Logging;
using Sparrow.Platform;
using Voron.Platform.Posix;

namespace Requests.Benchmark;

public partial class RavenDBInstance : IDisposable
{
    private const string DatabaseName = "Benchmark";
    private IDocumentStore Store;
    public RavenServer Server;
    public DocumentDatabase Database;
    private string _pathToServer = null;

    public void InitializeDatabase()
    {
        var configuration = RavenConfiguration.CreateForServer(Guid.NewGuid().ToString());
        configuration.Initialize();

        configuration.Server.Name = $"RequestsBenchmark";
        configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(60, TimeUnit.Seconds);
        configuration.Licensing.EulaAccepted = true;
        configuration.Logs.Mode = LogMode.None;
        configuration.Core.RunInMemory = false;
        configuration.Core.FeaturesAvailability = FeaturesAvailability.Experimental;
        configuration.Core.ServerUrls = new[] { "http://127.0.0.1:0" };
        _pathToServer = NewDataPath("RequestsBenchmark", 0, true);
        configuration.Core.DataDirectory = new PathSetting(_pathToServer);
        Server = new RavenServer(configuration)
        {
            ThrowOnLicenseActivationFailure = true,
            DebugTag = "A"
        };

        Server.Initialize();
        Server.ServerStore.ValidateFixedPort = false;
        AsyncHelpers.RunSync(() => Server.ServerStore.EnsureNotPassiveAsync());
        var dbPath = new PathSetting(Configuration.PathToDatabase);

        var doc = new DatabaseRecord(DatabaseName)
        {
            Settings =
            { 
                [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dbPath.ToFullPath(),
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = false.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true",
                [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString(),
                [RavenConfiguration.GetKey(x => x.Queries.RegexTimeout)] = (250).ToString(),
                [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = Configuration.PathToDatabase,
            }
        };

        Store = new DocumentStore()
        {
            Urls = [Server.WebUrl],
            Database = DatabaseName,

        };
        Store.Initialize();

        Store.Maintenance.Server.Send(new DeleteDatabasesOperation(DatabaseName, hardDelete: false));
        Store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

        Database = AsyncHelpers.RunSync(() => GetDatabase(Server, DatabaseName));
    }

    public void WaitForUserToContinue()
    {
        var urls = Store.Urls;
        var databaseNameEncoded = Uri.EscapeDataString(Store.Database);
        var address = urls.First() + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true&disableAnalytics=true";
        
        Console.WriteLine(address);
        
        do
        {
            Thread.Sleep(500);
        } while (Store.Commands(Store.Database).Head("Debug/Done") == null && Debugger.IsAttached);
    }



    public void Dispose()
    {
        var exceptions = new ExceptionAggregator("Found exceptions during dispose");
        exceptions.Execute(Store);
        exceptions.Execute(Server);
        DeletePath(_pathToServer, exceptions);
        exceptions.ThrowIfNeeded();
    }


}

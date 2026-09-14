using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Server;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Tests.Infrastructure;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27515 : RavenTestBase
{
    public RavenDB_27515(ITestOutputHelper output) : base(output)
    {
    }

    private const int NumberOfAlerts = 200;

    private const int AlertMessageSize = 128 * 1024;

    private const string AlertTitle = "RavenDB-27515";

    [RavenFact(RavenTestCategory.Core | RavenTestCategory.Voron)]
    public async Task Server_Notification_Center_Watch_Must_Not_Hold_A_Read_Transaction_While_Writing_To_A_Stalled_Client()
    {
        using var server = GetNewServer();

        AddAlerts(alert => server.ServerStore.NotificationCenter.Add(alert), database: null);

        await AssertStoredNotificationsReplayAsync(server, $"{server.WebUrl.Replace("http", "ws")}/server/notification-center/watch");
    }

    [RavenFact(RavenTestCategory.Core | RavenTestCategory.Voron)]
    public async Task Database_Notification_Center_Watch_Must_Not_Hold_A_Read_Transaction_While_Writing_To_A_Stalled_Client()
    {
        using var server = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = server });

        var database = await Databases.GetDocumentDatabaseInstanceFor(server, store);

        // the database notification center is backed by the server store's notifications storage
        // (AbstractDatabaseNotificationCenter uses ServerStore.NotificationCenter.Storage.GetStorageFor),
        // so this endpoint pins a read transaction on the very same System store
        AddAlerts(alert => database.NotificationCenter.Add(alert), store.Database);

        await AssertStoredNotificationsReplayAsync(server,
            $"{server.WebUrl.Replace("http", "ws")}/databases/{store.Database}/notification-center/watch");
    }

    private static void AddAlerts(Action<AlertRaised> add, string database)
    {
        // enough data that the replay cannot fit into the socket buffers, so the server side send really
        // parks once the client stops draining the connection
        var message = new string('a', AlertMessageSize);

        for (var i = 0; i < NumberOfAlerts; i++)
        {
            add(AlertRaised.Create(
                database,
                title: AlertTitle,
                msg: message,
                type: 0, // use any type
                severity: NotificationSeverity.Info,
                key: $"{AlertTitle}/{i}")); // distinct keys, otherwise the alerts would overwrite each other
        }
    }

    private static async Task AssertStoredNotificationsReplayAsync(RavenServer server, string url)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        using var client = new ClientWebSocket();

        await client.ConnectAsync(new Uri(url), cts.Token);

        var expectedMessage = new string('a', AlertMessageSize);
        var receivedKeys = new List<string>();

        void Collect(string message)
        {
            if (string.IsNullOrWhiteSpace(message)) // heartbeat
                return;

            var json = JObject.Parse(message);

            if (json[nameof(AlertRaised.Title)]?.Value<string>() != AlertTitle)
                return; // operations, cluster topology and the like

            // the notification is cloned out of the read transaction before it is sent, it must arrive intact
            Assert.Equal(expectedMessage, json[nameof(AlertRaised.Message)]?.Value<string>());

            var key = json[nameof(AlertRaised.Key)]?.Value<string>();
            Assert.NotNull(key);

            receivedKeys.Add(key);
        }

        // read a single notification, so we know the handler has reached the stored notifications replay
        Collect(await ReceiveMessageAsync(client, cts.Token));

        // from here on we deliberately never receive again, the receive window fills up and the server side
        // send stalls. nothing on the server bounds that wait - Kestrel's MinResponseDataRate is disabled by
        // default and there is no websocket keep alive - so the only thing that can release the read
        // transaction is not holding it across the network write in the first place
        await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);

        var openTransactions = GetStoredNotificationsReadTransactions(server);

        Assert.True(openTransactions.Count == 0,
            $"Expected no '{nameof(NotificationsStorage.ReadActionsOrderedByCreationDate)}' read transaction to be open on the System store " +
            $"while a stalled client is being written to, but found {openTransactions.Count}: " +
            string.Join(", ", openTransactions.Select(tx => $"tx {tx.Id} open for {DateTime.UtcNow - tx.TxStartTime}")));

        // now drain the rest, the replay is only correct if every stored notification is sent exactly once
        // and survives being cloned out of the read transaction
        while (receivedKeys.Count < NumberOfAlerts)
        {
            Collect(await ReceiveMessageAsync(client, cts.Token));
        }

        Assert.Equal(NumberOfAlerts, receivedKeys.Count);
        Assert.Equal(NumberOfAlerts, receivedKeys.Distinct().Count());

        for (var i = 0; i < NumberOfAlerts; i++)
        {
            Assert.Contains($"{AlertTitle}/{i}", receivedKeys);
        }
    }

    private static List<LowLevelTransaction> GetStoredNotificationsReadTransactions(RavenServer server)
    {
        return server.ServerStore._env.ActiveTransactions.AllTransactionsInstances
            .Where(tx => tx.CallerName == nameof(NotificationsStorage.ReadActionsOrderedByCreationDate))
            .ToList();
    }

    private static async Task<string> ReceiveMessageAsync(ClientWebSocket client, CancellationToken token)
    {
        var buffer = new ArraySegment<byte>(new byte[4096]);

        using var ms = new MemoryStream();

        while (true)
        {
            var result = await client.ReceiveAsync(buffer, token);

            Assert.NotEqual(WebSocketMessageType.Close, result.MessageType);

            ms.Write(buffer.Array!, 0, result.Count);

            if (result.EndOfMessage)
                return Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
        }
    }
}

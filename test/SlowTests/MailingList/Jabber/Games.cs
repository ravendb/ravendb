using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList.Jabber
{
    public class Games : RavenTestBase
    {
        private class GameServer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Blah { get; set; }

            public IList<ConnectedPlayer> ConnectedPlayers { get; set; }
        }

        private class ConnectedPlayer
        {
            public string PlayerName { get; set; }
            public string ConnectedOn { get; set; }
        }

        private class GameServers_ConnectedPlayers :
            AbstractIndexCreationTask<GameServer, GameServers_ConnectedPlayers.IndexQuery>
        {
            public GameServers_ConnectedPlayers()
            {
                Map = x => from s in x
                           from y in s.ConnectedPlayers
                           select new
                           {
                               ServerName = s.Name,
                               y.ConnectedOn,
                               y.PlayerName,
                               s.Id
                           };

                Store(x => x.ServerName, FieldStorage.Yes);
                Store(x => x.ConnectedOn, FieldStorage.Yes);
                Store(x => x.PlayerName, FieldStorage.Yes);
            }

            #region Nested type: IndexQuery

            public class IndexQuery
            {
                public string Id { get; set; }
                public string PlayerName { get; set; }
                public string ConnectedOn { get; set; }
                public string ServerName { get; set; }
            }

            #endregion
        }

        [Fact]
        public void CanUseTransformResults()
        {
            using (var store = GetDocumentStore())
            {
                SetupTestGameData(store);
                new GameServers_ConnectedPlayers().Execute(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    List<GameServers_ConnectedPlayers.IndexQuery> result =
                        session.Query<GameServers_ConnectedPlayers.IndexQuery, GameServers_ConnectedPlayers>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.PlayerName.StartsWith("p"))
                            .OrderBy(x => x.Id).ThenBy(x => x.PlayerName)
                            .ProjectInto<GameServers_ConnectedPlayers.IndexQuery>()
                            .ToList();


                    Assert.Equal(3, result.Count);

                    Assert.Equal("Phillip", result[0].PlayerName);
                    Assert.Equal("PK", result[1].PlayerName);
                    Assert.Equal("PewPewIsHere", result[2].PlayerName);


                    Assert.Equal("Jan 1", result[0].ConnectedOn);
                    Assert.Equal("Jan 2", result[1].ConnectedOn);
                    Assert.Equal("Jan 5", result[2].ConnectedOn);


                    Assert.Equal("Adsadsdasds", result[0].ServerName);
                    Assert.Equal("Adsadsdasds", result[1].ServerName);
                    Assert.Equal("Weeeee", result[2].ServerName);
                }
            }
        }

        public static void SetupTestGameData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var gameServer = session.Load<GameServer>("gameServers/1");
                if (gameServer != null)
                {
                    return;
                }

                session.Store(new GameServer
                {
                    Name = "Hi",
                    Blah = "banana",
                    ConnectedPlayers = new List<ConnectedPlayer>()
                });
                session.Store(new GameServer
                {
                    Name = "Adsadsdasds",
                    Blah = "apple",
                    ConnectedPlayers = new List<ConnectedPlayer>
                    {
                        new ConnectedPlayer
                        {PlayerName = "Phillip", ConnectedOn = "Jan 1"},
                        new ConnectedPlayer
                        {PlayerName = "PK", ConnectedOn = "Jan 2"},
                        new ConnectedPlayer
                        {PlayerName = "John", ConnectedOn = "Jan 3"}
                    }
                });
                session.Store(new GameServer
                {
                    Name = "Weeeee",
                    Blah = "banana",
                    ConnectedPlayers = new List<ConnectedPlayer>
                    {
                        new ConnectedPlayer
                        {
                            PlayerName = "PewPewIsHere",
                            ConnectedOn = "Jan 5"
                        },
                        new ConnectedPlayer
                        {PlayerName = "Killer", ConnectedOn = "Jan 2"}
                    }
                });

                session.SaveChanges();
            }
        }
    }
}

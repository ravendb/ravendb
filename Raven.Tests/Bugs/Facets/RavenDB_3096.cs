﻿using System;
 using System.Collections.Generic;
 using System.Linq;
 using Raven.Abstractions.Data;
 using Raven.Abstractions.Indexing;
 using Raven.Client;
 using Raven.Client.Document;
 using Raven.Client.Indexes;
 using Raven.Client.Shard;
 using Raven.Server;
 using Raven.Tests.Helpers;
 using Xunit;
 
 namespace RavenDbShardingTests
 {
     public class RavenDB_3096 : RavenTestBase
     {
         public class Profile
         {
             public string Id { get; set; }
             
             public string Name { get; set; }
 
             public string Location { get; set; }
         }
 
         public IDocumentStore NewRemoteDocumentStoreWithUrl(int port, bool fiddler = false, RavenDbServer ravenDbServer = null, string databaseName = null,
             bool runInMemory = true,
             string dataDirectory = null,
             string requestedStorage = null,
             bool enableAuthentication = false)
         {
             ravenDbServer = ravenDbServer ?? GetNewServer(runInMemory: runInMemory, dataDirectory: dataDirectory, requestedStorage: requestedStorage, enableAuthentication: enableAuthentication);
             ModifyServer(ravenDbServer);
             var store = new DocumentStore
             {
                 Url = GetServerUrl(port),
                 DefaultDatabase = databaseName,
             };
             stores.Add(store);
             store.AfterDispose += (sender, args) => ravenDbServer.Dispose();
             ModifyStore(store);
             return store.Initialize();
         }
 
         public class HybridShardingResolutionStrategy : DefaultShardResolutionStrategy
         {
             private readonly HashSet<Type> sharedTypes;
             private readonly string defaultShard;
 
             public HybridShardingResolutionStrategy(IEnumerable<string> shardIds, ShardStrategy shardStrategy,
                                                     IEnumerable<Type> sharedTypes, string defaultShard)
                 : base(shardIds, shardStrategy)
             {
                 this.sharedTypes = new HashSet<Type>(sharedTypes);
                 this.defaultShard = defaultShard;
             }
 
             public override string GenerateShardIdFor(object entity, ITransactionalDocumentSession sessionMetadata)
             {
                 if (!sharedTypes.Contains(entity.GetType()))
                     return ShardIds.FirstOrDefault(x => x == defaultShard);
 
                 return base.GenerateShardIdFor(entity, sessionMetadata);
             }
         }
 
         private static string GetServerUrl(int port)
         {
             return "http://localhost:" + port;
         }
 
         public class ProfileIndex: AbstractIndexCreationTask
         {
             public override IndexDefinition CreateIndexDefinition()
             {
                 return new IndexDefinition
                 {
                     Map = @"from profile in docs select new { profile.Id, profile.Name, profile.Location };",                    
                 };
             }
         }
         [Fact]
         public void ToFacetsDoesntWorkWithShardedDocumentSession()
         {
             using (var store1 = NewRemoteDocumentStoreWithUrl(8079, ravenDbServer: GetNewServer(8079)))
             {
                 using (var store2 = NewRemoteDocumentStoreWithUrl(8078, ravenDbServer: GetNewServer(8078)))
                 {
                     var shards = new List<IDocumentStore> { 
                         new DocumentStore { Identifier="Shard1", Url = store1.Url}, 
                         new DocumentStore { Identifier="Shard2", Url = store2.Url} }
                             .ToDictionary(x => x.Identifier, x => x);
 
                     var shardStrategy = new ShardStrategy(shards);
                     shardStrategy.ShardResolutionStrategy = new HybridShardingResolutionStrategy(shards.Keys, shardStrategy, new Type[0], "Shard1");
                     shardStrategy.ShardingOn<Profile>(x => x.Location);
 
                     using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
                     {
                         shardedDocumentStore.Initialize();
                         new ProfileIndex().Execute(shardedDocumentStore);
                         /*var facetSetup = new FacetSetup
                         {
                             Id = "facets/ProfileFacet",
                             Facets = new List<Facet>
                             {
                                 new Facet {Name = "Name", Mode = FacetMode.Default}
                             }
                         };*/
                         var facets = new List<Facet>
                         {
                             new Facet {Name = "Name", Mode = FacetMode.Default}
                         };
                         var profile = new Profile { Name = "Test", Location = "Shard1" };
 
                         using (var documentSession = shardedDocumentStore.OpenSession())
                         {
                             documentSession.Store(profile, profile.Id);
                             //documentSession.Store(facetSetup);
                             documentSession.SaveChanges();
                         }
                         using (var documentSession = shardedDocumentStore.OpenSession())
                         {
                             var query = documentSession.Query<Profile>("ProfileIndex").Where(x => x.Name == "Test");
                             var res = query.ToFacets(facets);
                             Assert.Equal(1,res.Results.Count);
                         }
                     }
                 }
             }
         }
     }
 }

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Claims;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19546 : RavenTestBase
    {
        public RavenDB_19546(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded, Skip = "Output reduce to collection isn't supported")]

        public void ShouldWork(Options options)
        {
            options.ModifyDocumentStore = x =>
            {
                x.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                {
                    CustomizeJsonDeserializer = s =>
                    {
                        s.Converters.Add(new IntIdConverter<UserAuth>((o, id) => o.Id = id));
                        s.Converters.Add(new IntIdConverter<UserAuthDetails>((o, id) => o.Id = id));
                    }
                };
                x.Conventions.FindIdentityProperty = conventionsFindIdentityProperty;
            };

            using (var store = GetDocumentStore(options))
            {
                new UserAndUserAuthDetails_Index().Execute(store);
                new UserAndUserAuthDetails_JavascriptIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "users/1",
                        FullName = "User with no claims",
                        ManuallyAttributedRoleIds = new List<string>() { "roles/1" }
                    }, "users/1");
                    session.Store(new User
                    {
                        Id = "users/2",
                        FullName = "User with one claim",
                        ManuallyAttributedRoleIds = new List<string>() { "roles/1" }
                    }, "users/2");
                    session.Store(new User
                    {
                        Id = "users/3",
                        FullName = "User with two claims",
                        ManuallyAttributedRoleIds = new List<string>() { "roles/1" }
                    }, "users/3");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    CreateAuthUser(session, 1);
                    CreateAuthUser(session, 2, new Claim("type", "value"));
                    CreateAuthUser(session, 3, new Claim("type", "value"), new Claim("unrelated", "value"));
                    session.SaveChanges();
                }
                var userAuthDetailsForUserIndexName = new UserAndUserAuthDetails_Index().IndexName;
                store.Maintenance.Send(new ResetIndexOperation(userAuthDetailsForUserIndexName));
                var userAuthDetailsForUserJavascriptIndexName = new UserAndUserAuthDetails_JavascriptIndex().IndexName;
                store.Maintenance.Send(new ResetIndexOperation(userAuthDetailsForUserJavascriptIndexName));
                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(10));

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var c = session.Query<UserClaims>().ToList().Count;
                    Assert.Equal(3, c);


                }
            }
        }
        private class UserClaims
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public IDictionary<string, SerializedClaim[]> ClaimsPerProvider { get; set; }
        }
        private class SerializedClaim
        {
            public string Type { get; set; }
            public string Value { get; set; }
            public string ValueType { get; set; }
            public string Issuer { get; set; }
            public string OriginalIssuer { get; set; }
        }
        private Func<MemberInfo, bool> conventionsFindIdentityProperty = prop =>
        {
            if (prop.Name != "Id")
                return false;

            if (prop.DeclaringType?.IsAssignableFrom(typeof(UserAuth)) == true)
                return false;
            if (prop.DeclaringType?.IsAssignableFrom(typeof(UserAuthDetails)) == true)
                return false;

            return true;
        };
        private static void CreateAuthUser(IDocumentSession session, int id, params Claim[] claims)
        {
            session.Store(new UserAuth { Id = id, RefIdStr = $"users/{id}" }, $"UserAuth/{id}");
            session.Store(
                new UserAuthDetails
                {
                    UserAuthId = id,
                    Provider = "idsrv",
                    Items = new Dictionary<string, string>()
                    {
                        {
                            "Claims",
                            FastTests.Blittable.StringExtensions.ToJsonString(claims.Select(x => new SerializedClaim()
                            {
                                Type = x.Type,
                                Value = x.Value,
                                ValueType = x.ValueType,
                                Issuer = x.Issuer,
                                OriginalIssuer = x.OriginalIssuer
                            }))
                        }
                    }
                }, $"UserAuthDetails/{id}");
        }


        private class User
        {
            public string Id { get; set; }
            public string Language { get; set; }
            public string Email { get; set; }

            public string FullName { get; set; }
            public IList<string> ManuallyAttributedRoleIds { get; set; }
        }

        private class UserAuthDetails
        {
            public int UserAuthId { get; set; }
            public int Id { get; set; }

            public virtual string Provider { get; set; }
            public virtual Dictionary<string, string> Items { get; set; } = new Dictionary<string, string>();
            public virtual DateTime ModifiedDate { get; set; }

        }

        private class UserAuth
        {
            public int Id { get; set; }
            public string RefIdStr { get; set; }
        }

        private class UserAndUserAuthDetails_Index : AbstractMultiMapIndexCreationTask<UserAndUserAuthDetails_Index.Result>
        {
            public class Result
            {
                public string UserId { get; set; }
                public string[] UserAuthDetailIds { get; set; }
            }

            public UserAndUserAuthDetails_Index()
            {
                AddMap<User>(users => from user in users
                                      select new Result
                                      {
                                          UserId = user.Id,
                                          UserAuthDetailIds = new string[] { }
                                      });
                AddMap<UserAuthDetails>(details => from userAuthDetails in details
                                                   let userAuth = LoadDocument<UserAuth>("UserAuths/" + userAuthDetails.UserAuthId)
                                                   select new Result
                                                   {
                                                       UserId = userAuth.RefIdStr,
                                                       UserAuthDetailIds = new[] { userAuthDetails.Id.ToString() }
                                                   });

                Reduce = results => from result in results
                                    group result by result.UserId
                    into g
                                    select new Result
                                    {
                                        UserId = g.Key,
                                        UserAuthDetailIds = g.SelectMany(r => r.UserAuthDetailIds).ToArray()
                                    };

                OutputReduceToCollection = "UserAndUserAuthDetails";

                Index(x => x.UserAuthDetailIds, FieldIndexing.No);
            }
        }

        private class UserAndUserAuthDetails_JavascriptIndex : AbstractJavaScriptIndexCreationTask
        {
            public UserAndUserAuthDetails_JavascriptIndex()
            {
                Maps = new HashSet<string>()
            {
                @"map('UserAndUserAuthDetails', function(auth){
var entity = load(auth.UserId, 'Users');
if (entity === null) return;

var claimsPerProvider = {};
var claimsModifiedDatePerProvider = {};

for (var i = 0; i < auth.UserAuthDetailIds.length; i++) {
    var id = auth.UserAuthDetailIds[i];
    var userAuthDetails = load('UserAuthDetails/' + id, 'UserAuthDetails');
    if (!userAuthDetails.Items.Claims) return;

    // which will cause the mappings for roles to use stale data
    if (claimsModifiedDatePerProvider[userAuthDetails.Provider] &&
        claimsModifiedDatePerProvider[userAuthDetails.Provider] > userAuthDetails.ModifiedDate)
        return;

    claimsPerProvider[userAuthDetails.Provider] = [];
    claimsModifiedDatePerProvider[userAuthDetails.Provider] = userAuthDetails.ModifiedDate;

    var claims = JSON.parse(userAuthDetails.Items.Claims);
    for (var j = 0; j < claims.length; j++) {
        var c = claims[j];
        claimsPerProvider[userAuthDetails.Provider].push(c);
    }
}
                    var userClaim = {};
                    userClaim.UserId = auth.UserId;
                    userClaim.ClaimsPerProvider = claimsPerProvider;
                    return userClaim;
                })"
            };

                Reduce = @"groupBy(x => ({UserId: x.UserId, ClaimsPerProvider: x.ClaimsPerProvider}))
            .aggregate(g => {
            return {
                UserId: g.key.UserId,
                ClaimsPerProvider: g.key.ClaimsPerProvider
            }
        })";

                OutputReduceToCollection = "UserClaims";

                Fields = new Dictionary<string, IndexFieldOptions>() { { "ClaimsPerProvider", new IndexFieldOptions() { Indexing = FieldIndexing.No } } };
            }
        }



        private class IntIdConverter<T> : JsonConverter where T : new()
        {
            private readonly Action<T, int> _idSetter;

            public IntIdConverter(Action<T, int> idSetter) => _idSetter = idSetter;

            public override bool CanConvert(Type objectType) => objectType == typeof(T);

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var item = JObject.Load(reader);
                var entity = new T();
                serializer.Populate(item.CreateReader(), entity);
                var id = item[Constants.Documents.Metadata.Key]?[Constants.Documents.Metadata.Id]?.ToString();
                _idSetter(entity, ToIntId(id));
                return entity;
            }

            public override bool CanRead => true;
            public override bool CanWrite => false;

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                throw new NotImplementedException();
            }
        }
        public static int ToIntId(string id)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (!id.Contains("/"))
                throw new InvalidOperationException($"'{id}' does not look like a RavenDB ID, missing a /");
            return int.Parse(id.Split('/')[1]);
        }
    }
}

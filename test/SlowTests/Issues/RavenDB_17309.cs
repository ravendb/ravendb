using System.Collections.Generic;
using System.Linq;
using FastTests.Server.Replication;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17309 : ReplicationTestBase
    {
        public RavenDB_17309(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void QueryWithSelectAfterSelectNew()
        {
            using (var store = GetDocumentStore())
            {
                const string userId = "users/1";
                const string profileId = "usersProfile/1";
                var user = new User
                {
                    Id = userId,
                    ProfileId = profileId,
                    Location = new Location
                    {
                        Latitude = 24.2,
                        Longitude = 81.9
                    }
                };
                var userProfile = new UserProfile
                {
                    Id = profileId,
                    MiniText = "mini text",
                    Title = "profile of users/1",
                    AvatarUrl = "http://user1.avatar.com"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(user, userId);
                    session.Store(userProfile, profileId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new
                        {
                            Profile = RavenQuery.Load<UserProfile>(u.ProfileId),
                            User = u
                        })
                        .Select(model => new ProBamboo
                        {
                            ProfileId = model.Profile.Id,
                            UserId = model.User.Id,
                            Title = model.Profile.Title,
                            MiniText = model.Profile.MiniText,
                            Latitude = model.User.Location.Latitude,
                            Longitude = model.User.Location.Longitude,
                            AvatarUrl = model.Profile.AvatarUrl
                        });

                    var bamboo = query.First();

                    Assert.Equal(profileId, bamboo.ProfileId);
                    Assert.Equal(userId, bamboo.UserId);
                    Assert.Equal(userProfile.Title, bamboo.Title);
                    Assert.Equal(userProfile.MiniText, bamboo.MiniText);
                    Assert.Equal(user.Location.Latitude, bamboo.Latitude);
                    Assert.Equal(user.Location.Longitude, bamboo.Longitude);
                    Assert.Equal(userProfile.AvatarUrl, bamboo.AvatarUrl);
                }
            }
        }

        [Fact]
        public void QueryWithSelectAfterSelectNew_WithNestedLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1",
                        Employee = "employees/1",
                        Freight = 3
                    });
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Discount = 5
                            },
                            new OrderLine
                            {
                                Discount = 3
                            }
                        }
                    }, "companies/1");
                    session.Store(new Order(), "employees/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Where(o => o.Id.StartsWith("orders/"))
                        .Select(order => new
                        {
                            Model = order.Freight == 3 ? RavenQuery.Load<Order>(order.Company) : RavenQuery.Load<Order>(order.Employee),
                            Order = order
                        })
                        .Select(x => new
                        {
                            Freight = x.Order.Freight,
                            HasModel = x.Model != null ? x.Model.Lines.Where(y => y.Discount == x.Order.Freight).FirstOrDefault() != null : false
                        });

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Freight);
                    Assert.True(result[0].HasModel);
                }
            }
        }

        private class ProBamboo
        {
            public string ProfileId { get; set; }
            public string UserId { get; set; }
            public string Title { get; set; }
            public string MiniText { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
            public string AvatarUrl { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string ProfileId { get; set; }
            public Location Location { get; set; }
        }

        private class Location
        {
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }


        private class UserProfile
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string AvatarUrl { get; set; }
            public string MiniText { get; set; }
        }
    }
}

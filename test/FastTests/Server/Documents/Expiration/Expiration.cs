//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Expiration
{
    public class Expiration : ExpirationTest
    {
        public Expiration()
        {
            SystemTime.UtcDateTime = () => DateTime.UtcNow;
        }

        [Fact]
        public async Task CanAddEntityWithExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry()
        {
            using (var store = await GetDocumentStore())
            {
                await SetupExpiration(store);

                var company = new Company {Name = "Company Name"};
                var expiry = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    session.Advanced.GetMetadataFor(company)[Constants.Expiration.RavenExpirationDate] = new RavenJValue(expiry.ToString(Default.DateTimeOffsetFormatsToWrite));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    Assert.NotNull(company2);
                    var metadata = session.Advanced.GetMetadataFor(company2);
                    var expirationDate = metadata["Raven-Expiration-Date"];
                    Assert.NotNull(expirationDate);
                    var dateTime = expirationDate.Value<DateTime>();
                    Assert.Equal(DateTimeKind.Utc, dateTime.Kind);
                    Assert.Equal(expiry, expirationDate);
                }

                // Can_add_entity_with_expiry
                SystemTime.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    Assert.NotNull(company2);
                }
            }
        }

        public override void Dispose()
        {
            SystemTime.UtcDateTime = null;
            base.Dispose();
        }
    }
}
//-----------------------------------------------------------------------
// <copyright file="DynamicQueries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Globalization;
using System.Threading;
using Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Queries;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Queries
{
    public class DynamicQueries : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public DynamicQueries()
		{
			db = new DocumentDatabase(new RavenConfiguration
				{
					DataDirectory = "raven.db.test.esent",
				});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

        [Fact]
        public void CanPerformQueryToSelectSingleItem()
        {
            db.Put("ayende", null, RavenJObject.FromObject(new {Name = "Ayende"}), new RavenJObject(), null);
            
            var result = db.ExecuteQueryUsingLinearSearch(new LinearQuery
            {
                Query = "from doc in docs select new { doc.Name }"
            });

            Assert.Empty(result.Errors);
            Assert.Equal(@"{""Name"":""Ayende"",""__document_id"":""ayende""}", result.Results[0].ToString(Formatting.None));
        }

        [Fact]
        public void CanSelectFullDocument()
        {
            db.Put("ayende", null, RavenJObject.FromObject(new { Name = "Ayende" }), new RavenJObject(), null);

            var result = db.ExecuteQueryUsingLinearSearch(new LinearQuery
            {
                Query = "from doc in docs select doc"
            });

            Assert.Empty(result.Errors);
            var jObject = result.Results[0];
            jObject.Remove("@metadata");
            Assert.Equal(@"{""Name"":""Ayende""}", jObject.ToString(Formatting.None));
        }


        [Fact]
        public void CanGetErrorsInQueries()
        {
            db.Put("ayende", null, RavenJObject.FromObject(new { Name = "Ayende" }), new RavenJObject(), null);
            db.Put("rahien", null, RavenJObject.FromObject(new { Username = "Ayende" }), new RavenJObject(), null);
            db.Put("oren", null, RavenJObject.FromObject(new { Name = "Ayende" }), new RavenJObject(), null);

            var oldCulture = Thread.CurrentThread.CurrentCulture;
            var oldUiCulture = Thread.CurrentThread.CurrentUICulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            var result = db.ExecuteQueryUsingLinearSearch(new LinearQuery
            {
                Query = "from doc in docs select new { L = 1/doc.Name.Length } "
            });

            Thread.CurrentThread.CurrentUICulture = oldUiCulture;
            Thread.CurrentThread.CurrentCulture = oldCulture;


            Assert.Equal("Doc 'rahien', Error: Operator '/' cannot be applied to operands of type 'int' and 'Raven.Abstractions.Linq.DynamicNullObject'", result.Errors[0]);
            Assert.Equal(@"{""L"":0,""__document_id"":""ayende""}", result.Results[0].ToString(Formatting.None));
            Assert.Equal(@"{""L"":0,""__document_id"":""oren""}", result.Results[1].ToString(Formatting.None));
        }
	}
}

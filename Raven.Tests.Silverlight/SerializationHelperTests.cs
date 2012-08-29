using Raven.Abstractions;
using Raven.Client.Connection;
using Raven.Json.Linq;

namespace Raven.Tests.Silverlight
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	public class SerializationHelperTests : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> Handles_conversion_when_there_is_no_metadata()
		{
			var input = new List<RavenJObject> {new RavenJObject()};
			var output = SerializationHelper.RavenJObjectsToJsonDocuments(input);

			Assert.AreEqual(1, output.Count());

			yield break;
		}

		[TestMethod]
		public void Extracts_key_from_metadata()
		{
			var doc = new RavenJObject();
			doc["@metadata"] = new RavenJObject();
			((RavenJObject)doc["@metadata"])["@id"] = "some_key";

			var output = SerializationHelper.RavenJObjectsToJsonDocuments(new List<RavenJObject> { doc });

			Assert.AreEqual("some_key", output.First().Key);
		}

		[TestMethod]
		public void Assumes_empty_string_if_key_is_not_in_metadata()
		{
			var doc = new RavenJObject();
			doc["@metadata"] = new RavenJObject();

			var output = SerializationHelper.RavenJObjectsToJsonDocuments(new List<RavenJObject> { doc });

			Assert.AreEqual(string.Empty, output.First().Key);
		}

		[TestMethod]
		public void Extracts_last_modified_date_from_metadata()
		{
			var april_fools = new DateTime(2011, 4, 1, 4, 20, 0, DateTimeKind.Utc);


			var doc = new RavenJObject();
			doc["@metadata"] = new RavenJObject();
			((RavenJObject)doc["@metadata"])["Last-Modified"] = april_fools.ToString("r");

			var output = SerializationHelper.RavenJObjectsToJsonDocuments(new List<RavenJObject> { doc });

			Assert.AreEqual(april_fools, output.First().LastModified);
		}

		[TestMethod]
		public void Assumes_now_if_last_modified_date_is_not_in_metadata()
		{
			var doc = new RavenJObject();
			doc["@metadata"] = new RavenJObject();

			var now = SystemTime.UtcNow;

			var output = SerializationHelper.RavenJObjectsToJsonDocuments(new List<RavenJObject> { doc });

			var last_modified = output.First().LastModified;
			var delta = Math.Abs((last_modified - now).Value.TotalSeconds);

			Assert.IsTrue(delta < 1);
		}

		[TestMethod]
		public void Extracts_etag_from_metadata()
		{
			var etag = Guid.NewGuid();

			var doc = new RavenJObject();
			doc["@metadata"] = new RavenJObject();
			((RavenJObject)doc["@metadata"])["@etag"] = etag.ToString();

			var output = SerializationHelper.RavenJObjectsToJsonDocuments(new List<RavenJObject> { doc });

			Assert.AreEqual(etag, output.First().Etag);
		}

		[TestMethod]
		public void Assumes_empty_guid_if_etag_is_not_in_metadata()
		{
			var doc = new RavenJObject();
			doc["@metadata"] = new RavenJObject();

			var output = SerializationHelper.RavenJObjectsToJsonDocuments(new List<RavenJObject> { doc });

			Assert.AreEqual(Guid.Empty, output.First().Etag);
		}

		[TestMethod]
		public void Extracts_Non_Authoritative_flag_from_metadata()
		{
			var doc = new RavenJObject();
			doc["@metadata"] = new RavenJObject();
			((RavenJObject)doc["@metadata"])["Non-Authoritative-Information"] = true;

			var output = SerializationHelper.RavenJObjectsToJsonDocuments(new List<RavenJObject> { doc });

			Assert.AreEqual(true, output.First().NonAuthoritativeInformation);
		}

		[TestMethod]
		public void Assumes_false_if_Non_Authoritative_flag_is_not_in_metadata()
		{
			var doc = new RavenJObject();
			doc["@metadata"] = new RavenJObject();

			var output = SerializationHelper.RavenJObjectsToJsonDocuments(new List<RavenJObject> { doc });

			Assert.AreEqual(false, output.First().NonAuthoritativeInformation);
		}
	}
}
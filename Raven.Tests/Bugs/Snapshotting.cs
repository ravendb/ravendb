using System;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Snapshotting
	{
		[Fact]
 		public void ModifyingSnapshotNestedArrayWillNotChangeOtherSnapshots()
		{
			var rjo = new RavenJObject
			{
				{"tests", new RavenJArray()}
			};

			rjo["a"] = 1;

			var ravenJToken = (RavenJObject)rjo.CloneToken();
			ravenJToken.EnsureCannotBeChangeAndEnableSnapshotting();

			var first = (RavenJObject)ravenJToken.CreateSnapshot();
			((RavenJArray)first["tests"]).Add(1);


			var second = (RavenJObject)ravenJToken.CreateSnapshot();
			((RavenJArray)second["tests"]).Add(3);

			Assert.Equal(1, ((RavenJArray)second["tests"]).Length);
		}

		[Fact]
		public void ModifyingSnapshotNestedArrayWillNotChangeParent()
		{
			var rjo = new RavenJObject
			{
				{"tests", new RavenJArray()}
			};

			rjo["a"] = 1;

			var ravenJToken = (RavenJObject)rjo.CloneToken();
			ravenJToken.EnsureCannotBeChangeAndEnableSnapshotting();

			var first = (RavenJObject)ravenJToken.CreateSnapshot();
			((RavenJArray)first["tests"]).Add(1);


			var second = (RavenJObject)ravenJToken.CreateSnapshot();
			((RavenJArray)second["tests"]).Add(3);

			Assert.Equal(0, ((RavenJArray)rjo["tests"]).Length);
		}
	}
}
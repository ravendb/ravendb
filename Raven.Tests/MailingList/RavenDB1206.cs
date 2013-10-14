using System;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class RavenDB1206 : RavenTest
	{
		//TODO: return test
		//[Fact]
		//public void SeedIdentityLocal()
		//{
		//	using (var store = NewDocumentStore()) {
		//		store.DatabaseCommands.SeedIdentityFor("chairs", 44);

		//		Assert.Equal(45, store.DatabaseCommands.NextIdentityFor("chairs"));
		//		Assert.Equal(46, store.DatabaseCommands.NextIdentityFor("chairs"));
		//	}
		//}

		//[Fact]
		//public void SeedIdentityRemote()
		//{
		//	using (var store = NewRemoteDocumentStore()) {
		//		store.DatabaseCommands.SeedIdentityFor("chairs", 44);

		//		Assert.Equal(45, store.DatabaseCommands.NextIdentityFor("chairs"));
		//		Assert.Equal(46, store.DatabaseCommands.NextIdentityFor("chairs"));
		//	}
		//}

		//[Fact]
		//public void SeedIdentityLocal_WithIdentityAlreadyGenerated()
		//{
		//	using (var store = NewDocumentStore()) {
		//		Assert.Equal(1, store.DatabaseCommands.NextIdentityFor("chairs"));
		//		Assert.Equal(2, store.DatabaseCommands.NextIdentityFor("chairs"));

		//		store.DatabaseCommands.SeedIdentityFor("chairs", 44);

		//		Assert.Equal(45, store.DatabaseCommands.NextIdentityFor("chairs"));
		//		Assert.Equal(46, store.DatabaseCommands.NextIdentityFor("chairs"));
		//	}
		//}

		//[Fact]
		//public void SeedIdentityRemote_WithIdentityAlreadyGenerated()
		//{
		//	using (var store = NewRemoteDocumentStore()) {
		//		Assert.Equal(1, store.DatabaseCommands.NextIdentityFor("chairs"));
		//		Assert.Equal(2, store.DatabaseCommands.NextIdentityFor("chairs"));

		//		store.DatabaseCommands.SeedIdentityFor("chairs", 44);

		//		Assert.Equal(45, store.DatabaseCommands.NextIdentityFor("chairs"));
		//		Assert.Equal(46, store.DatabaseCommands.NextIdentityFor("chairs"));
		//	}
		//}
	}
}
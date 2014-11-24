using System.Net;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Mark : RavenTest
	{
		[Fact]
		public void InvalidCharsInPath()
		{
			using(GetNewServer())
			{
				var request = WebRequest.Create(@"http://localhost:8079/indexes/dynamic?query=(@metadata.Raven-Entity-Name:Briefs%20AND%20@metadata.Customer-Id:16)%20AND%20(InitiationBrief.Confidential:false%20OR%20(InitiationBrief.Confidential:true%20AND%20(InitiationBrief.WordingOwner.Id:6860%20OR%20InitiationBrief.BrandCopyProvider.Id:6860%20OR%20InitiationBrief.CategoryCopyProvider.Id:6860%20OR%20InitiationBrief.FormulationCopyProvider.Id:6860%20OR%20InitiationBrief.TechnicalCopyProvider.Id:6860%20OR%20InitiationBrief.SourcingCopyProvider.Id:6860%20OR%20InitiationBrief.QualityTeamCopyProvider.Id:6860%20OR%20InitiationBrief.BIBrandingCopyProvider.Id:6860%20OR%20InitiationBrief.BIRegistrationCopyProvider.Id:6860%20OR%20InitiationBrief.PackagingCopyProvider.Id:6860%20OR%20InitiationBrief.ThirdPartySupplierCopyProvider.Id:6860%20OR%20InitiationBrief.GuestStarBrandManagerCopyProvider.Id:6860%20OR%20InitiationBrief.ArtworkZenBriefer.Id:6860)))%20AND%20(LogosPosition.Attachment:*white*)");
				request.GetResponse().Close();
			}
		}
	}
}
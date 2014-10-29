using System;

using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1470 : NoDisposalNeeded
	{
		[Fact]
		public void Etag_to_Guid_conversion()
		{
			var etag = new Raven.Abstractions.Data.Etag("01234567-8901-2345-6789-012345678901");
			var guid = (Guid) etag;
			var nullabelGuid = (Guid?)etag;
			Assert.Equal(etag.ToString(), guid.ToString());
			Assert.Equal(etag.ToString(), nullabelGuid.ToString());
		}

		[Fact]
		public void Guid_to_Etag_conversion()
		{
			var guid = new Guid("01234567-8901-2345-6789-012345678901");
			var nullableGuid = (Guid?)guid;
			Assert.Equal(guid.ToString(), ((Raven.Abstractions.Data.Etag)guid).ToString());
			Assert.Equal(guid.ToString(), ((Raven.Abstractions.Data.Etag)nullableGuid).ToString());
		}

		[Fact]
		public void Etag_guid_comparision()
		{
			var etag = new Raven.Abstractions.Data.Etag("01234567-8901-2345-6789-012345678901");
			var guid = new Guid("01234567-8901-2345-6789-012345678901");
			Assert.True(etag.Equals(guid));
			Assert.True(guid.Equals(etag));
		}

		[Fact]
		public void Etag_converted_to_Guid_then_to_string_and_back_yields_the_same_Etag()
		{
			var originalEtag = new Raven.Abstractions.Data.Etag(UuidType.Documents, 12, 12);

			var etagAsGuid = (Guid)originalEtag;
			var etagAfterConversionToString = etagAsGuid.ToString();

			var etagAfterGuidAndStringConversion = Raven.Abstractions.Data.Etag.Parse(etagAfterConversionToString);

			Assert.Equal(originalEtag, etagAfterGuidAndStringConversion);
		}

		[Fact]
		public void Etag_converted_to_string_then_to_Guid_and_back_yields_the_same_Etag()
		{
			var originalEtag = new Raven.Abstractions.Data.Etag(UuidType.Documents, 12, 12);

			var etagAsString = originalEtag.ToString();
			var etagAsStringConvertedToGuid = Guid.Parse(etagAsString);

			var etagAfterConversion = (Raven.Abstractions.Data.Etag)etagAsStringConvertedToGuid;

			Assert.Equal(originalEtag, etagAfterConversion);
		}
	}
}

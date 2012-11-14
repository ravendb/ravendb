using System.Web;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.MoreLikeThis;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace Raven.Tests.Bundles.MoreLikeThis
{
	public class MoreLikeThisQueryParametersTests
	{
		[Fact]
		public void CanEncodeDecodeRequestByDocumentId()
		{
			var parameters = new MoreLikeThisQuery
			{
				IndexName = "dataIndex",
				DocumentId = "foo/1",
				Fields = new[] {"Body"},
				MinimumWordLength = 3,
				MinimumDocumentFrequency = 1,
				Boost = true,
			};

			var uri = parameters.GetRequestUri();
			Assert.Equal("/morelikethis/?index=dataIndex&docid=foo%2F1&fields=Body&boost=true&minDocFreq=1&minWordLen=3&", uri);

			var path = uri.Substring(0, uri.IndexOf('?'));
			var queryString = HttpUtility.ParseQueryString(uri.Substring(uri.IndexOf('?')));
			var decodedParameters = MoreLikeThisResponder.GetParametersFromPath(path, queryString);

			Assert.Equal("dataIndex", decodedParameters.IndexName);
			Assert.Equal(JsonConvert.SerializeObject(parameters), JsonConvert.SerializeObject(decodedParameters));
		}

		[Fact]
		public void CanEncodeDecodeRequestOnIndexGrouping()
		{
			var parameters = new MoreLikeThisQuery
			{
				IndexName = "dataIndex",
				Fields = new[] {"Body"},
				MinimumWordLength = 3,
				MinimumDocumentFrequency = 1,
				Boost = true,
			};
			parameters.MapGroupFields.Add("foo", "bar");
			parameters.MapGroupFields.Add("be", "bop");

			var uri = parameters.GetRequestUri();

			Assert.Equal("/morelikethis/?index=dataIndex&docid=foo%3Dbar%3Bbe%3Dbop&fields=Body&boost=true&minDocFreq=1&minWordLen=3&", uri);

			var path = uri.Substring(0, uri.IndexOf('?'));
			var queryString = HttpUtility.ParseQueryString(uri.Substring(uri.IndexOf('?')));
			var decodedParameters = MoreLikeThisResponder.GetParametersFromPath(path, queryString);

			Assert.Equal("dataIndex", decodedParameters.IndexName);
			Assert.Equal(JsonConvert.SerializeObject(parameters), JsonConvert.SerializeObject(decodedParameters));
		}
	}
}
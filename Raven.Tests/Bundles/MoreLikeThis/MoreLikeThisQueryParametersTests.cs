using System.Web;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace Raven.Bundles.Tests.MoreLikeThis
{
	public class MoreLikeThisQueryParametersTests
	{
		[Fact]
		public void Can_encode_decode_request_by_documentId()
		{
			var parameters = new MoreLikeThisQueryParameters();

			parameters.IndexName = "dataIndex";
			parameters.DocumentId = "foo/1";
			parameters.Fields = new[] { "Body" };
			parameters.MinimumWordLength = 3;
			parameters.MinimumDocumentFrequency = 1;
			parameters.Boost = true;

			var uri = parameters.GetRequestUri(parameters.IndexName);

			Assert.Equal("/morelikethis/?index=dataIndex&docid=foo%2F1&fields=Body&boost=true&minDocFreq=1&minWordLen=3&", uri);

			var path = uri.Substring(0, uri.IndexOf('?'));
			var queryString = HttpUtility.ParseQueryString(uri.Substring(uri.IndexOf('?')));
			var decodedParameters = MoreLikeThisQueryParameters.GetParametersFromPath(path, queryString);

			Assert.Equal("dataIndex", decodedParameters.IndexName);
			Assert.Equal(JsonConvert.SerializeObject(parameters), JsonConvert.SerializeObject(decodedParameters));
		}

		[Fact]
		public void Can_encode_decode_request_on_index_grouping()
		{
			var parameters = new MoreLikeThisQueryParameters();

			parameters.IndexName = "dataIndex";
			parameters.MapGroupFields.Add("foo", "bar");
			parameters.MapGroupFields.Add("be", "bop");
			parameters.Fields = new[] { "Body" };
			parameters.MinimumWordLength = 3;
			parameters.MinimumDocumentFrequency = 1;
			parameters.Boost = true;

			var uri = parameters.GetRequestUri(parameters.IndexName);

			Assert.Equal("/morelikethis/?index=dataIndex&docid=foo%3Dbar%3Bbe%3Dbop&fields=Body&boost=true&minDocFreq=1&minWordLen=3&", uri);

			var path = uri.Substring(0, uri.IndexOf('?'));
			var queryString = HttpUtility.ParseQueryString(uri.Substring(uri.IndexOf('?')));
			var decodedParameters = MoreLikeThisQueryParameters.GetParametersFromPath(path, queryString);

			Assert.Equal("dataIndex", decodedParameters.IndexName);
			Assert.Equal(JsonConvert.SerializeObject(parameters), JsonConvert.SerializeObject(decodedParameters));
		}
	}
}

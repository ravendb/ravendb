using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Raven.Bundles.MoreLikeThis;
using Xunit;

namespace Raven.Bundles.Tests.MoreLikeThis
{
	public class MoreLikeThisQueryParametersTests
	{
		[Fact]
		public void Can_encode_decode_request_by_documentId()
		{
			var parameters = new MoreLikeThisQueryParameters();

			parameters.DocumentId = "foo/1";
			parameters.Fields = new[] { "Body" };
			parameters.MinimumWordLength = 3;
			parameters.MinimumDocumentFrequency = 1;
			parameters.Boost = true;

			var uri = parameters.GetRequestUri("dataIndex");

			Assert.Equal("/morelikethis/dataIndex/foo/1?fields=Body&boost=true&minDocFreq=1&minWordLen=3&", uri);

			var path = uri.Substring(0, uri.IndexOf('?'));

			string indexName;
			var decodedParameters = MoreLikeThisQueryParameters.GetParametersFromPath(uri, out indexName);

			Assert.Equal("dataIndex", indexName);
			Assert.Equal(JsonConvert.SerializeObject(parameters), JsonConvert.SerializeObject(decodedParameters));
		}

		[Fact]
		public void Can_encode_decode_request_on_index_grouping()
		{
			var parameters = new MoreLikeThisQueryParameters();

			parameters.MapGroupFields.Add("foo", "bar");
			parameters.MapGroupFields.Add("be", "bop");
			parameters.Fields = new[] { "Body" };
			parameters.MinimumWordLength = 3;
			parameters.MinimumDocumentFrequency = 1;
			parameters.Boost = true;

			var uri = parameters.GetRequestUri("dataIndex");

			Assert.Equal("/morelikethis/dataIndex/foo=bar;be=bop?fields=Body&boost=true&minDocFreq=1&minWordLen=3&", uri);

			var path = uri.Substring(0, uri.IndexOf('?'));

			string indexName;
			var decodedParameters = MoreLikeThisQueryParameters.GetParametersFromPath(uri, out indexName);

			Assert.Equal("dataIndex", indexName);
			Assert.Equal(JsonConvert.SerializeObject(parameters), JsonConvert.SerializeObject(decodedParameters));
		}
	}
}

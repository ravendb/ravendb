//-----------------------------------------------------------------------
// <copyright file="MoreLikeThisResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;

namespace Raven.Bundles.MoreLikeThis
{
	public class MoreLikeThisResponder : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/morelikethis/([\w\-_]+)/(.+)"; } // /morelikethis/(index-name)/(ravendb-document-id)
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var indexName = match.Groups[1].Value;
			var documentId = match.Groups[2].Value;
			
			IndexSearcher searcher;
			using (Database.IndexStorage.GetCurrentIndexSearcher(indexName, out searcher))
			{
				var td = searcher.Search(new TermQuery(new Term(Constants.DocumentIdFieldName, documentId)), 1); // get the current Lucene docid for the given RavenDB doc ID
				if (td.ScoreDocs.Length == 1) // only do this if the doc was found in the specified index
				{
					var mlt = new Similarity.Net.MoreLikeThis(searcher.GetIndexReader());
					mlt.SetAnalyzer(new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29)); // TODO: Determine correct analyzer programmatically
					mlt.SetFieldNames(context.Request.QueryString["fields"].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)); // what fields to base the MLT operation on
					// TODO: Figure out other relevant configs. It will probably make sense to use a config doc, like in facets

					byte maxDocs;
					if (!byte.TryParse(context.Request.QueryString["maxDocs"], out maxDocs))
						maxDocs = 10;

					var mltQuery = mlt.Like(td.ScoreDocs[0].doc);
					var tsdc = TopScoreDocCollector.create(maxDocs, true);
					searcher.Search(mltQuery, tsdc);
					var hits = tsdc.TopDocs().ScoreDocs;

					var retSet = new HashSet<JsonDocument>();
					foreach (var hit in hits)
					{
						var docId = searcher.Doc(hit.doc).Get(Constants.DocumentIdFieldName);
						var doc = Database.Get(docId, null);
						if (doc != null)
							retSet.Add(doc);
					}

					context.WriteJson(retSet);
				}
			}
		}
	}
}

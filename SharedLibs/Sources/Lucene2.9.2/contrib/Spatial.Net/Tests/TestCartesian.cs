/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Spatial.GeoHash;
using Lucene.Net.Spatial.Geometry;
using Lucene.Net.Spatial.Tier;
using Lucene.Net.Spatial.Tier.Projectors;
using Lucene.Net.Store;
using Lucene.Net.Util;
using NUnit.Framework;

namespace Tests
{
	[TestFixture]
	public class TestCartesian
	{
		private Directory _directory;
		private IndexSearcher _searcher;
		// reston va
		private double _lat = 38.969398;
		private double _lng = -77.386398;
		private const string LatField = "lat";
		private const string LngField = "lng";
		private readonly List<CartesianTierPlotter> _ctps = new List<CartesianTierPlotter>();

		private readonly IProjector _projector = new SinusoidalProjector();

		[SetUp]
		protected void SetUp()
		{
			_directory = new RAMDirectory();

			var writer = new IndexWriter(_directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);

			SetUpPlotter(2, 15);

			AddData(writer);
		}

		private void SetUpPlotter(int @base, int top)
		{

			for (; @base <= top; @base++)
			{
				_ctps.Add(new CartesianTierPlotter(@base, _projector, CartesianTierPlotter.DefaltFieldPrefix));
			}
		}

		private void AddData(IndexWriter writer)
		{
			AddPoint(writer, "McCormick &amp; Schmick's Seafood Restaurant", 38.9579000, -77.3572000);
			AddPoint(writer, "Jimmy's Old Town Tavern", 38.9690000, -77.3862000);
			AddPoint(writer, "Ned Devine's", 38.9510000, -77.4107000);
			AddPoint(writer, "Old Brogue Irish Pub", 38.9955000, -77.2884000);
			AddPoint(writer, "Alf Laylah Wa Laylah", 38.8956000, -77.4258000);
			AddPoint(writer, "Sully's Restaurant &amp; Supper", 38.9003000, -77.4467000);
			AddPoint(writer, "TGI Friday", 38.8725000, -77.3829000);
			AddPoint(writer, "Potomac Swing Dance Club", 38.9027000, -77.2639000);
			AddPoint(writer, "White Tiger Restaurant", 38.9027000, -77.2638000);
			AddPoint(writer, "Jammin' Java", 38.9039000, -77.2622000);
			AddPoint(writer, "Potomac Swing Dance Club", 38.9027000, -77.2639000);
			AddPoint(writer, "WiseAcres Comedy Club", 38.9248000, -77.2344000);
			AddPoint(writer, "Glen Echo Spanish Ballroom", 38.9691000, -77.1400000);
			AddPoint(writer, "Whitlow's on Wilson", 38.8889000, -77.0926000);
			AddPoint(writer, "Iota Club and Cafe", 38.8890000, -77.0923000);
			AddPoint(writer, "Hilton Washington Embassy Row", 38.9103000, -77.0451000);
			AddPoint(writer, "HorseFeathers, Bar & Grill", 39.01220000000001, -77.3942);
			AddPoint(writer, "Marshall Island Airfield", 7.06, 171.2);
			AddPoint(writer, "Midway Island", 25.7, -171.7);
			AddPoint(writer, "North Pole Way", 55.0, 4.0);

			writer.Commit();
			writer.Close();
		}

		private void AddPoint(IndexWriter writer, String name, double lat, double lng)
		{
			Document doc = new Document();

			doc.Add(new Field("name", name, Field.Store.YES, Field.Index.ANALYZED));

			// convert the lat / long to lucene fields
			doc.Add(new Field(LatField, NumericUtils.DoubleToPrefixCoded(lat), Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field(LngField, NumericUtils.DoubleToPrefixCoded(lng), Field.Store.YES, Field.Index.NOT_ANALYZED));

			// add a default meta field to make searching all documents easy 
			doc.Add(new Field("metafile", "doc", Field.Store.YES, Field.Index.ANALYZED));

			int ctpsize = _ctps.Count;
			for (int i = 0; i < ctpsize; i++)
			{
				CartesianTierPlotter ctp = _ctps[i];
				var boxId = ctp.GetTierBoxId(lat, lng);
				doc.Add(new Field(ctp.GetTierFieldName(),
								  NumericUtils.DoubleToPrefixCoded(boxId),
								  Field.Store.YES,
								  Field.Index.NOT_ANALYZED_NO_NORMS));
			}
			writer.AddDocument(doc);

		}

		[Test]
		public void TestAntiM()
		{
			_searcher = new IndexSearcher(_directory, true);

			const double miles = 6.0;

			Console.WriteLine("testAntiM");
			// create a distance query
			var dq = new DistanceQueryBuilder(_lat, _lng, miles, LatField, LngField, CartesianTierPlotter.DefaltFieldPrefix, true);

			Console.WriteLine(dq);
			//create a term query to search against all documents
			Query tq = new TermQuery(new Term("metafile", "doc"));

			var dsort = new DistanceFieldComparatorSource(dq.DistanceFilter);
			Sort sort = new Sort(new SortField("foo", dsort, false));

			// Perform the search, using the term query, the distance filter, and the
			// distance sort
			TopDocs hits = _searcher.Search(tq, dq.Filter, 1000, sort);
			int results = hits.totalHits;
			ScoreDoc[] scoreDocs = hits.scoreDocs;

			// Get a list of distances
			Dictionary<int, Double> distances = dq.DistanceFilter.Distances;

			
			Console.WriteLine("Distance Filter filtered: " + distances.Count);
			Console.WriteLine("Results: " + results);
			Console.WriteLine("=============================");
			Console.WriteLine("Distances should be 7 " + distances.Count);
			Console.WriteLine("Results should be 7 " + results);

			Assert.AreEqual(7, distances.Count); // fixed a store of only needed distances
			Assert.AreEqual(7, results);

			double lastDistance = 0;
			for (int i = 0; i < results; i++)
			{
				Document d = _searcher.Doc(scoreDocs[i].doc);

				String name = d.Get("name");
				double rsLat = NumericUtils.PrefixCodedToDouble(d.Get(LatField));
				double rsLng = NumericUtils.PrefixCodedToDouble(d.Get(LngField));
				Double geo_distance = distances[scoreDocs[i].doc];

				double distance = DistanceUtils.GetInstance().GetDistanceMi(_lat, _lng, rsLat, rsLng);
				double llm = DistanceUtils.GetInstance().GetLLMDistance(_lat, _lng, rsLat, rsLng);

				Console.WriteLine("Name: " + name + ", Distance " + distance);
				
				Assert.IsTrue(Math.Abs((distance - llm)) < 1);
				Assert.IsTrue((distance < miles));
				Assert.IsTrue(geo_distance >= lastDistance);
				
				lastDistance = geo_distance;
			}
		}
	}
}

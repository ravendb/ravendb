// -----------------------------------------------------------------------
//  <copyright file="Vitaliy.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Vitaliy : RavenTest
	{
			public class Cgc
		{
			public Guid Id { get; set; }
			public string Name { get; set; }
		}

		public class Production
		{
			public Guid Id { get; set; }
			public Guid CgcId { get; set; }
			public string Name { get; set; }
		}

		public class CgcAndProduction
		{
			public Guid ProductionId { get; set; }
			public Guid CgcId { get; set; }
			public string CgcName { get; set; }
			public string ProductionName { get; set; }
		}

		public class CgcAndProductionIndex : AbstractMultiMapIndexCreationTask<CgcAndProduction>
		{
			public CgcAndProductionIndex()
			{
				AddMap<Cgc>(enumerable => enumerable
												.Select(x => new
																{
																	ProductionId = (string) null,
																	CgcId =
																Guid.Parse(x.Id.ToString().Replace("cgcs/", "")),
																	CgcName = x.Name,
																	ProductionName = (string) null,
																}));

				AddMap<Production>(enumerable => enumerable
														.Select(x => new
																		{
																			ProductionId =
																		Guid.Parse(x.Id.ToString().Replace(
																			"productions/", "")),
																			x.CgcId,
																			CgcName = (string) null,
																			ProductionName = x.Name,
																		}));

				Reduce = results => results
										.GroupBy(x => x.CgcId)
										.Select(x => new
															{
																ProductionId =
															x.Select(d => d.ProductionId).FirstOrDefault(d => d != null),
																CgcId = x.Key,
																CgcName =
															x.Select(d => d.CgcName).FirstOrDefault(d => d != null),
																ProductionName =
															x.Select(d => d.ProductionName).FirstOrDefault(d => d != null),
															});
			}
		}

		[Fact]
		public void Run()
		{
			using (var documentStore = NewDocumentStore())
			{

				using (var documentSession = documentStore.OpenSession())
				{
					var cgc = new Cgc {Name = "Cgc"};
					documentSession.Store(cgc);

					var production = new Production {Name = "Production", CgcId = cgc.Id};
					documentSession.Store(production);

					var cgc2 = new Cgc {Name = "TwoCgc"};
					documentSession.Store(cgc2);

					var production2 = new Production {Name = "TwoProduction", CgcId = cgc2.Id};
					documentSession.Store(production2);

					documentSession.SaveChanges();
				}

				new CgcAndProductionIndex().Execute(documentStore);

				using (var documentSession = documentStore.OpenSession())
				{
					var results = documentSession
						.Query<CgcAndProduction, CgcAndProductionIndex>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.CgcName == "TwoCgc" && x.ProductionName == "TwoProduction")
						.ToList();

					Assert.Equal(1, results.Count);
				}
			}
		}
	}
}
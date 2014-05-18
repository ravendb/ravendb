// -----------------------------------------------------------------------
//  <copyright file="LongIds.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class LongIds : RavenTest
	{
		[Fact]
		public void Embedded()
		{
			using (var store = NewDocumentStore())
			{
				{
					var customer = new TestCustomer
					{
						Id = "LoremipsumdolorsitametconsecteturadipiscingelitPraesentlobortisconguecursusCurabiturconvallisnuncmattisliberomolestieidiaculismagnaimperdietDuisnecenimsednislvestibulumvulputateDonecnuncarcumolestieeutinciduntacfermentumpretiumestAeneannoncondimentumorciDonecsitametanteerossedgravidaestQuisqueturpismaurisplaceratsedaliquamidgravidasednislIntegermetusleoultriciesegetiaculisnonporttitornonlacusProinegetfringillalectusCrasfeugiatloremaauctoregestasmienimpulvinarsemquisbibendumloremvelitnonnullaDonecultriciesfelissednunctinciduntutrutrumtellusmolestieIntegerliberorisusvariusinvehiculaidtristiqueidarcNuncpretiummolestieduicongueauctorloremcursussitametCurabituridmassaeratcursusadipiscingvelitNullaminmaurisestsitametpretiumnislSedmollisultriciespurusNuncaerosnislnonmollislacusIntegerlaciniavariuscommodoNamrutrumerossitametni"
					};
					using (IDocumentSession session = store.OpenSession())
					{
						session.Store(customer);
						session.SaveChanges();
					}

					// This works
					using (var session = store.OpenSession())
					{
						IEnumerable<TestCustomer> actual = session.Query<TestCustomer>().Customize(x=>x.WaitForNonStaleResults())
							.ToArray();
						Assert.Equal(customer.Id, actual.Single().Id);
					}

					// This fails with invalid operation exception 
					using (IDocumentSession session = store.OpenSession())
					{
						var loadedCustomer = session.Load<TestCustomer>(customer.Id);
						Assert.NotNull(loadedCustomer);
					}
				}
			}
		}

		[Fact]
		public void Remote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				{
					var customer = new TestCustomer
					{
						Id = "LoremipsumdolorsitametconsecteturadipiscingelitPraesentlobortisconguecursusCurabiturconvallisnuncmattisliberomolestieidiaculismagnaimperdietDuisnecenimsednislvestibulumvulputateDonecnuncarcumolestieeutinciduntacfermentumpretiumestAeneannoncondimentumorciDonecsitametanteerossedgravidaestQuisqueturpismaurisplaceratsedaliquamidgravidasednislIntegermetusleoultriciesegetiaculisnonporttitornonlacusProinegetfringillalectusCrasfeugiatloremaauctoregestasmienimpulvinarsemquisbibendumloremvelitnonnullaDonecultriciesfelissednunctinciduntutrutrumtellusmolestieIntegerliberorisusvariusinvehiculaidtristiqueidarcNuncpretiummolestieduicongueauctorloremcursussitametCurabituridmassaeratcursusadipiscingvelitNullaminmaurisestsitametpretiumnislSedmollisultriciespurusNuncaerosnislnonmollislacusIntegerlaciniavariuscommodoNamrutrumerossitametni"
					};
					using (IDocumentSession session = store.OpenSession())
					{
						session.Store(customer);
						session.SaveChanges();
					}

					// This works
					using (var session = store.OpenSession())
					{
						IEnumerable<TestCustomer> actual = session.Query<TestCustomer>()
							.Customize(x => x.WaitForNonStaleResults()).ToArray();
						Assert.Equal(customer.Id, actual.Single().Id);
					}

					// This fails with invalid operation exception 
					using (IDocumentSession session = store.OpenSession())
					{
						var loadedCustomer = session.Load<TestCustomer>(customer.Id);
						Assert.NotNull(loadedCustomer);
					}
				}
			}
		}

		public class TestCustomer
		{
			public string Id { get; set; }
		}
	}
}
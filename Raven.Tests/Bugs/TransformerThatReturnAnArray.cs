using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class TransformerThatReturnAnArray : RavenTest
	{
		[Fact]
		public void ShouldReturnNullForNotExistDocument()
		{
			using (var store = NewDocumentStore())
			{
				new BigContainerReferenceTransformer().Execute(store);

				using (var session = store.OpenSession())
				{
					StoreSampleData(session);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
                    var existId = session.Load<BigContainerReferenceTransformer, BigContainer[]>("BigContainer1");
                    Assert.Equal(4, existId.Length);

                    // It should return null rather then an empty array, so the user can distinguish between a not exist document and an empty result of a transformer function. 
					var notExistId = session.Load<BigContainerReferenceTransformer, BigContainer[]>("BigContainerThatDoesNotExist");
                    Assert.Null(notExistId);
				}
			}
		}

		public class BigContainer
		{
			public string Id { get; set; }
			public IList<SmallContainer> SmallContainers { get; set; }
		}

		public class SmallContainer
		{
			public Reference Reference { get; set; }
		}

		public class Reference
		{
			public string BigContainerId { get; set; }
		}

		public class BigContainerReferenceTransformer : AbstractTransformerCreationTask<BigContainer>
		{
			public BigContainerReferenceTransformer()
			{
			    TransformResults = bigContainers => bigContainers
			        .SelectMany(bigContainer => Recurse(bigContainer, bigContainerRecurse => bigContainerRecurse.SmallContainers
			                                                                                                    .Where(smallContainer => smallContainer.Reference != null)
			                                                                                                    .Select(smallContainer => LoadDocument<BigContainer>(smallContainer.Reference.BigContainerId))));
			}
		}

		public static void StoreSampleData(IDocumentSession documentSession)
		{
			documentSession.Store(new BigContainer
			{
				Id = "BigContainer1",
				SmallContainers = new[]
				{
					new SmallContainer
					{
						Reference = new Reference
						{
							BigContainerId = "BigContainer2"
						}
					}
				}
			});

			documentSession.Store(new BigContainer
			{
				Id = "BigContainer2",
				SmallContainers = new[]
				{
					new SmallContainer
					{
						Reference = new Reference
						{
							BigContainerId = "BigContainer3"
						}
					},
					new SmallContainer
					{
						Reference = new Reference
						{
							BigContainerId = "BigContainer4"
						}
					}
				}
			});

			documentSession.Store(new BigContainer
			{
				Id = "BigContainer3",
				SmallContainers = new[]
				{
					new SmallContainer()
				}
			});

			documentSession.Store(new BigContainer
			{
				Id = "BigContainer4",
				SmallContainers = new[]
				{
					new SmallContainer
					{
						Reference = new Reference
						{
							BigContainerId = "BigContainer1"
						}
					}
				}
			});

			documentSession.Store(new BigContainer
			{
				Id = "BigContainer5",
				SmallContainers = new[]
				{
					new SmallContainer
					{
						Reference = new Reference
						{
							BigContainerId = "BigContainer1"
						}
					}
				}
			});
		}
	}
}
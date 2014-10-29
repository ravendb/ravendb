// -----------------------------------------------------------------------
//  <copyright file="Lindblom.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Lindblom : RavenTest
	{
		[Fact]
		public void Test()
		{
			// Arrange
			IPageModel data;

			// Act
			using (var store = NewDocumentStore())
			{
				new AllPages().Execute(store);
				using (var session = store.OpenSession())
				{
					var pageModel = new PageModel
					{
						Parent = null
					};
					session.Store(pageModel);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					data = session.Query<IPageModel, AllPages>()
					              .Customize(x => x.WaitForNonStaleResults())
					              .SingleOrDefault(x => x.Parent == null);
				}
			}

			// Assert
			Assert.NotNull(data);
		}

		public class DocumentReference<T> where T : IPageModel
		{
			/// <summary>
			/// Get/Sets the Id of the DocumentReference
			/// </summary>
			/// <value></value>
			public string Id { get; set; }

			/// <summary>
			/// Get/Sets the Slug of the DocumentReference
			/// </summary>
			/// <value></value>
			public string Slug { get; set; }

			/// <summary>
			/// Gets or sets the URL.
			/// </summary>
			/// <value>
			/// The URL.
			/// </value>
			public string Url { get; set; }

			/// <summary>
			/// Implicitly converts a page model to a DocumentReference
			/// </summary>
			/// <param name="document"></param>
			/// <returns></returns>
			public static implicit operator DocumentReference<T>(T document)
			{
				return new DocumentReference<T>
				{
					Id = document.Id
				};
			}
		}

		public interface IPageModel
		{
			string Id { get; set; }
			DocumentReference<IPageModel> Parent { get; set; }
		}

		public class PageModel : IPageModel
		{
			public string Id { get; set; }
			public DocumentReference<IPageModel> Parent { get; set; }
		}

		public class AllPages : AbstractMultiMapIndexCreationTask<IPageModel>
		{
			/// <summary>
			/// Initializes a new instance of the <see cref="AllPages"/> class.
			/// </summary>
			public AllPages()
			{
				AddMapForAll<IPageModel>(pages => from page in pages
												  select new
												  {
													  page.Id,
													  page.Parent
												  });
			}
		}
	}
}
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Extensions;

namespace Raven.Studio.Models
{
	public class HomeModel : PageViewModel
	{
		private DocumentsModel recentDocuments;

		public DocumentsModel RecentDocuments
		{
			get
			{
				if (recentDocuments == null)
				{
				    recentDocuments = (new DocumentsModel(new DocumentsCollectionSource())
				                                                      {
				                                                          Header = "Recent Documents",
                                                                          DocumentNavigatorFactory = (id, index) => DocumentNavigator.Create(id, index),
                                                                          Context = "AllDocuments",
				                                                      });
                    recentDocuments.SetChangesObservable(d => d.DocumentChanges.Select(s => Unit.Default));
				}

				return recentDocuments;
			}
		}

		public HomeModel()
		{
			ModelUrl = "/home";
		}

	    private bool isGeneratingSampleData;
		public bool IsGeneratingSampleData
		{
			get { return isGeneratingSampleData; }
			set { isGeneratingSampleData = value; OnPropertyChanged(() => IsGeneratingSampleData); }
		}

		#region Commands

		public ICommand CreateSampleData
		{
			get { return new CreateSampleDataCommand(null); }
		}

		#endregion
	}
}
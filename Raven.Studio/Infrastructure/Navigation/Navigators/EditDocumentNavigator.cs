using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Studio.Commands;

namespace Raven.Studio.Infrastructure.Navigation.Navigators
{
	[NavigatorExport(@"^docs/(?<id>.*)", Index = 30)]
	public class EditDocumentNavigator : BaseNavigator
	{
		private readonly EditDocumentById editDocumentById;

		[ImportingConstructor]
		public EditDocumentNavigator(EditDocumentById editDocumentById)
		{
			this.editDocumentById = editDocumentById;
		}

		protected override void OnNavigate(Dictionary<string, string> parameters)
		{
			editDocumentById.Execute(parameters["id"]);
		}
	}
}
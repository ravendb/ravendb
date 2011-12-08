// -----------------------------------------------------------------------
//  <copyright file="GetDocumentByIdCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class NavigateToDocumentByIdCommand : Command
	{
		public override void Execute(object _)
		{
			AskUser.QuestionAsync("Edit Document By ID", "Document ID?")
				.ContinueOnSuccessInTheUIThread(id => UrlUtil.Navigate("/edit?id=" + id));
		}	
	}
}
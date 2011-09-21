using System;
using System.Threading.Tasks;

namespace Raven.Studio.Features.Input
{
	public class AskUser
	{
		public static Task<string> QuestionAsync(string title, string question)
		{
			var dataContext = new InputModel
			{
				Title = title,
				Question = question
			};
			var inputWindow = new InputWindow
			{
				DataContext = dataContext
			};

			var tcs = new TaskCompletionSource<string>();

			inputWindow.Closed += (sender, args) =>
			{
				if (inputWindow.DialogResult == true)
					tcs.SetResult(dataContext.Answer);
				else
					tcs.SetCanceled();
			};

			inputWindow.Show();

			return tcs.Task;
		}

		public static Task<bool> ConfirmationAsync(string title, string question)
		{
			var dataContext = new ConfirmModel
			{
				Title = title,
				Question = question
			};
			var inputWindow = new ConfirmWindow
			{
				DataContext = dataContext
			};

			var tcs = new TaskCompletionSource<bool>();

			inputWindow.Closed += (sender, args) =>
			{
				if (inputWindow.DialogResult != null)
					tcs.SetResult(inputWindow.DialogResult.Value);
				else
					tcs.SetCanceled();
			};

			inputWindow.Show();

			return tcs.Task;
		}
	}
}
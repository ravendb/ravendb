using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Input
{
	public class InputModel : NotifyPropertyChangedBase
	{
		private string title;
		public string Title
		{
			get { return title; }
			set
			{
				title = value;
				OnPropertyChanged(() => Title);
			}
		}

		private string question;
		public string Question
		{
			get { return question; }
			set
			{
				question = value;
				OnPropertyChanged(()=>Question);
			}
		}

		private string answer;
		public string Answer
		{
			get { return answer; }
			set
			{
				answer = value;
				OnPropertyChanged(() => Answer);
			}
		}
	}
}
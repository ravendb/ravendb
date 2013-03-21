namespace Raven.Tests.Bugs.TransformResults
{
	public class QuestionView
	{
		public string QuestionId { get; set; }
		public string UserDisplayName { get; set; }
		public string QuestionTitle { get; set; }
		public string QuestionContent { get; set; }
		public int VoteTotal { get; set; }
		public User User { get; set; }
		public Question Question { get; set; }
	}
}

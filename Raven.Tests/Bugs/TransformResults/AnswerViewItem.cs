namespace Raven.Tests.Bugs.TransformResults
{
	public class AnswerViewItem
	{
		public string QuestionId { get; set; }
		public string AnswerId { get; set; }
		public string Content { get; set; }
		public string UserId { get; set; }
		public string UserDisplayName { get; set; }
		public int VoteTotal { get; set; }
	    public decimal DecimalTotal { get; set; }
	}
}
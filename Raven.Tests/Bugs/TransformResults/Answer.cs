using System;

namespace Raven.Tests.Bugs.TransformResults
{
	public class Answer
	{
		public string Id { get; set; }
		public string UserId { get; set; }
		public string QuestionId { get; set; }
		public string Content { get; set; }
	}
	public class Answer2
	{
		public Guid Id { get; set; }
		public string UserId { get; set; }
		public Guid QuestionId { get; set; }
		public string Content { get; set; }
		public AnswerVote2[] Votes { get;set; }
	}
}
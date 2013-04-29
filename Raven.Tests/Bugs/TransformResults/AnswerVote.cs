using System;

namespace Raven.Tests.Bugs.TransformResults
{
	public class AnswerVote
	{
		public string QuestionId { get; set; }
		public string AnswerId { get; set; }
		public int Delta { get; set; }
		public decimal DecimalValue { get; set; }
	}
	public class AnswerVote2
	{
		public Guid Id { get; set; }
		public Guid QuestionId { get; set; }
		public Guid AnswerId { get; set; }
		public int Delta { get; set; }
	}

}
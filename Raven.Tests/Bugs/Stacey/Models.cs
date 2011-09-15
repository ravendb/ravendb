using System.Collections.Generic;

namespace Raven.Tests.Bugs.Stacey
{
	public abstract class Entity
	{
		public string Id { get; set; }
		public string Name { get; set; }
	}

	public class Aspect : Entity
	{
		public Kind Kind { get; set; }
		public Category Category { get; set; }
		public Path Path { get; set; }
	}

	/// <summary>
	/// Denotes the aspect's polymorphed form.
	/// </summary>
	public enum Kind
	{
		None,
		Statistic,
		Attribute,
		Skill
	}

	public enum Category
	{
		None = 0,
		Physical = 1,
		Mental = 2,
		Spirit = 3
	}

	public class Step
	{
		public int Cost { get; set; }
		public int Number { get; set; }
		public string Currency { get; set; }
		public List<Requirement> Requirements { get; set; }
	}

	public class Currency : Entity
	{
	}

	public class Path
	{
		public List<Step> Steps { get; set; }
	}

	public class Requirement
	{
		public string What { get; set; }
		public int Value { get; set; }
	}
}
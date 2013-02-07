using System;

namespace Raven.Tests.MonoForAndroid.Models
{
	public class TestItem
	{
		public bool Selected { get; set; }
		public string Name { get; set; }
		public Action Action { get; set; }
	}
}
using System;
using System.Collections.Generic;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Util;
using Raven.Client.Counters;

namespace Raven.Tryouts
{
	public class Program
	{
		private static void Main()
		{
			using (var counterStore = new CounterStore
			{
				Url = "http://localhost:8080",
				Name = "TestCounterStore",				
			})
			{
				counterStore.Initialize(true);				
	
				AsyncHelpers.RunSync(() => counterStore.IncrementAsync("G", "C1"));

				AsyncHelpers.RunSync(() => counterStore.IncrementAsync("G", "C2"));
				AsyncHelpers.RunSync(() => counterStore.IncrementAsync("G", "C2"));

				AsyncHelpers.RunSync(() => counterStore.IncrementAsync("G2", "C1"));

				AsyncHelpers.RunSync(() => counterStore.ChangeAsync("G2", "C2", 26));
			}
		}
	}
}

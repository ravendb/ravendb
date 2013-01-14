// -----------------------------------------------------------------------
//  <copyright file="Users_Stats_ByCountry.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.SimulatedWorkLoad.Model;

namespace Raven.SimulatedWorkLoad.Indexes
{
	public class Users_Stats_ByState : AbstractIndexCreationTask<User, Users_Stats_ByState.Result>
	{
		public class Result
		{
			public string State { get; set; }
			public int Count { get; set; }
		}
		public Users_Stats_ByState()
		{
			Map = users =>
				  from user in users
				  select new
				  {
					  user.State,
					  Count = 1
				  };
			Reduce = results =>
					 from result in results
					 group result by result.State
						 into g
						 select new
						 {
							 State = g.Key,
							 Count = g.Sum(x => x.Count)
						 };
		}
	}

	public class Users_Stats_ByStateAndcity : AbstractIndexCreationTask<User, Users_Stats_ByStateAndcity.Result>
	{
		public class Result
		{
			public string State { get; set; }
			public string City { get; set; }
			public int Count { get; set; }
		}
		public Users_Stats_ByStateAndcity()
		{
			Map = users =>
				  from user in users
				  select new
				  {
					  user.State,
					  user.City,
					  Count = 1
				  };
			Reduce = results =>
					 from result in results
					 group result by new { result.State , result.City}
						 into g
						 select new
						 {
							 g.Key.State,
							 g.Key.City,
							 Count = g.Sum(x => x.Count)
						 };
		}
	}
}
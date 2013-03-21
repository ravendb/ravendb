//-----------------------------------------------------------------------
// <copyright file="CustomDynamicClass.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Dynamic;

namespace Raven.Tests.Document
{
	public class CustomDynamicClass : DynamicObject
	{
		readonly Dictionary<string, object> dictionary = new Dictionary<string, object>();

		public int Count
		{
			get { return dictionary.Count; }
		}

		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			string name = binder.Name;            

			return dictionary.TryGetValue(name, out result);
		}

		public override bool TrySetMember(SetMemberBinder binder, object value)
		{            
			dictionary[binder.Name] = value;            
			return true;
		}

		public override IEnumerable<string> GetDynamicMemberNames()
		{
			return dictionary.Keys;
		}
	}    
}

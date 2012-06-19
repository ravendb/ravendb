//-----------------------------------------------------------------------
// <copyright file="RecordingComparer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Munin
{
	public class RecordingComparer : IComparer<IComparable>
	{
		public IComparable LastComparedTo { get; set; }

		public int Compare(IComparable x, IComparable y)
		{
			LastComparedTo = x;
			return x.CompareTo(y);
		}
	}
}
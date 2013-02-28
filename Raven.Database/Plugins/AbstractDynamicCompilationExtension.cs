//-----------------------------------------------------------------------
// <copyright file="AbstractDynamicCompilationExtension.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.Reflection;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractDynamicCompilationExtension
	{
		public abstract string[] GetNamespacesToImport();
		public abstract string[] GetAssembliesToReference();
	}
}

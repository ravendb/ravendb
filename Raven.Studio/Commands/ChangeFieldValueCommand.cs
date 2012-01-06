// -----------------------------------------------------------------------
//  <copyright file="ChangeFieldValueCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class ChangeFieldValueCommand<T> : Command
	{
		private readonly T model;
		private readonly Action<T> action;

		public ChangeFieldValueCommand(T model, Action<T> action)
		{
			this.model = model;
			this.action = action;
		}

		public override bool CanExecute(object parameter)
		{
			return action != null;
		}

		public override void Execute(object parameter)
		{
			action(model);
		}
	}
}
namespace Raven.Studio.Framework
{
	using System;
	using System.Windows;
	using System.Windows.Input;
	using System.Windows.Interactivity;
	using Caliburn.Micro;
	using EventTrigger = System.Windows.Interactivity.EventTrigger;

	/// <summary>
	/// https://gist.github.com/704830
	/// http://blog.caraulean.com/2010/11/18/handling-mouse-double-click-event-in-silverlight-with-caliburn-micro/
	/// </summary>
	public static class DoubleClickEvent
	{
		public static readonly DependencyProperty AttachActionProperty =
			DependencyProperty.RegisterAttached(
				"AttachAction",
				typeof (string),
				typeof (DoubleClickEvent),
				new PropertyMetadata(OnAttachActionChanged));

		public static void SetAttachAction(DependencyObject d, string attachText)
		{
			d.SetValue(AttachActionProperty, attachText);
		}

		public static string GetAttachAction(DependencyObject d)
		{
			return d.GetValue(AttachActionProperty) as string;
		}

		static void OnAttachActionChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			if (e.NewValue == e.OldValue)
				return;

			var text = e.NewValue as string;
			if (string.IsNullOrEmpty(text))
				return;

			AttachActionToTarget(text, d);
		}

		static void AttachActionToTarget(string text, DependencyObject d)
		{
			var actionMessage = Parser.CreateMessage(d, text);

			var trigger = new ConditionalEventTrigger
			              	{
			              		EventName = "MouseLeftButtonUp",
			              		Condition = e => DoubleClickCatcher.IsDoubleClick(d, e)
			              	};
			trigger.Actions.Add(actionMessage);

			Interaction.GetTriggers(d).Add(trigger);
		}

		#region Nested type: ConditionalEventTrigger

		public class ConditionalEventTrigger : EventTrigger
		{
			public Func<EventArgs, bool> Condition { get; set; }

			protected override void OnEvent(EventArgs eventArgs)
			{
				if (Condition(eventArgs))
					base.OnEvent(eventArgs);
			}
		}

		#endregion

		#region Nested type: DoubleClickCatcher

		static class DoubleClickCatcher
		{
			const int AllowedPositionDelta = 6;
			const int DoubleClickSpeed = 400;

			static Point clickPosition;
			static bool firstClickDone;
			static DateTime lastClick = DateTime.Now;

			internal static bool IsDoubleClick(object sender, EventArgs args)
			{
				var element = sender as UIElement;
				var clickTime = DateTime.Now;

				var e = args as MouseEventArgs;
				if (e == null)
					throw new ArgumentException("MouseEventArgs expected");

				var span = clickTime - lastClick;

				if (span.TotalMilliseconds > DoubleClickSpeed || firstClickDone == false)
				{
					clickPosition = e.GetPosition(element);
					firstClickDone = true;
					lastClick = DateTime.Now;
					return false;
				}

				firstClickDone = false;
				var position = e.GetPosition(element);
				if (Math.Abs(clickPosition.X - position.X) < AllowedPositionDelta &&
				    Math.Abs(clickPosition.Y - position.Y) < AllowedPositionDelta)
					return true;
				return false;
			}
		}

		#endregion
	}
}
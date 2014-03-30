package net.ravendb.abstractions.basic;

import java.util.List;

import net.ravendb.abstractions.closure.Action1;


public class EventHelper {

  /**
   * Helper used for invoking event on list of delegates
   * @param delegates
   * @param sender
   * @param event
   */
  public static <T extends EventArgs> void invoke(List<EventHandler<T>> delegates, Object sender, T event) {
    for (EventHandler<T> delegate : delegates) {
      delegate.handle(sender, event);
    }
  }

  public static <T> void invoke(List<Action1<T>> actions, T argument) {
    for (Action1<T> action: actions) {
      action.apply(argument);
    }
  }
}

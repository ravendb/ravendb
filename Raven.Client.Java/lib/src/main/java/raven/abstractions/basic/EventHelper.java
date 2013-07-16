package raven.abstractions.basic;

import java.util.List;

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
}

package raven.abstractions.basic;

import java.util.List;

public class EventHelper {
  public static <T extends EventArgs> void invoke(List<EventHandler<T>> delegates, Object sender, T event) {
    for (EventHandler<T> delegate : delegates) {
      delegate.handle(sender, event);
    }
  }
}

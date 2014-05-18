package net.ravendb.abstractions.basic;

/**
 * Represents tuple
 * @param <T>
 * @param <S>
 */
public class Tuple<T, S> {
  public static <T, S> Tuple<T, S> create(T item1, S item2) {
    return new Tuple<>(item1, item2);
  }

  private T item1;
  private S item2;

  public Tuple(T item1, S item2) {
    this.item1 = item1;
    this.item2 = item2;
  }

  public Tuple() {

  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tuple other = (Tuple) obj;
    if (item1 == null) {
      if (other.item1 != null)
        return false;
    } else if (!item1.equals(other.item1))
      return false;
    if (item2 == null) {
      if (other.item2 != null)
        return false;
    } else if (!item2.equals(other.item2))
      return false;
    return true;
  }

  /**
   * @return the item1
   */
  public T getItem1() {
    return item1;
  }

  /**
   * @return the item2
   */
  public S getItem2() {
    return item2;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((item1 == null) ? 0 : item1.hashCode());
    result = prime * result + ((item2 == null) ? 0 : item2.hashCode());
    return result;
  }

  /**
   * @param item1 the item1 to set
   */
  public void setItem1(T item1) {
    this.item1 = item1;
  }

  /**
   * @param item2 the item2 to set
   */
  public void setItem2(S item2) {
    this.item2 = item2;
  }

}

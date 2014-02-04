package net.ravendb.client;


public class RavenPagingInformation {

  private int start;
  private int pageSize;
  private int nextPageStart;

  public void fill(int start, int pageSize, int nextPageStart) {
    if (start < 0) {
      throw new IllegalStateException("Start must be greather or equals than 0.");
    }
    if (pageSize <= 0) {
      throw new IllegalArgumentException("PageSize must be greather than 0.");
    }

    this.start = start;
    this.pageSize = pageSize;
    this.nextPageStart = nextPageStart;
  }

  public boolean isForPreviousPage(int start, int pageSize) {
    if (this.pageSize != pageSize) {
      return false;
    }
    return isLastPage() == false && this.start + pageSize == start;
  }

  public boolean isLastPage() {
    return getStart() == getNextPageStart();
  }


  public int getStart() {
    return start;
  }


  public int getPageSize() {
    return pageSize;
  }


  public int getNextPageStart() {
    return nextPageStart;
  }
}

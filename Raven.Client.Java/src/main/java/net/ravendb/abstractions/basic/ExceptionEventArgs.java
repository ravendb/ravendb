package net.ravendb.abstractions.basic;


public class ExceptionEventArgs extends EventArgs {

  public ExceptionEventArgs(Exception exception) {
    super();
    this.exception = exception;
  }
  private Exception exception;

  public Exception getException() {
    return exception;
  }


  public void setException(Exception exception) {
    this.exception = exception;
  }

}

package raven.client.connection;

import java.util.Stack;
import java.util.UUID;

import raven.abstractions.data.TransactionInformation;

public class RavenTransactionAccessor {
  private RavenTransactionAccessor() {
    // empty by design
  }

  private static ThreadLocal<Stack<TransactionInformation>> currentRavenTransactions = new ThreadLocal<>();
  private static ThreadLocal<Boolean> supressExplicitRavenTransaction = new ThreadLocal<>();

  private static Stack<TransactionInformation> getCurrentRavenTransactions() {
    if (currentRavenTransactions.get() == null) {
      currentRavenTransactions.set(new Stack<TransactionInformation>());
    }
    return currentRavenTransactions.get();
  }

  private static boolean isSupressExplicitRavenTransaction() {
    if (supressExplicitRavenTransaction.get() == null) {
      supressExplicitRavenTransaction.set(false);
    }
    return supressExplicitRavenTransaction.get();
  }

  /**
   * Starts a transaction
   * @return
   */
  public static AutoCloseable startTransaction() {
    return startTransaction((long)60 * 1000);
  }

  /**
   * Starts a transaction with the specified timeout
   * @param timeout timeout in milis
   * @return
   */
  public static AutoCloseable startTransaction(long timeoutInMilis)
  {
    TransactionInformation transInfo = new TransactionInformation();
    transInfo.setId(UUID.randomUUID().toString());
    transInfo.setTimeout(timeoutInMilis);
    getCurrentRavenTransactions().push(transInfo);
    return new AutoCloseable() {

      @Override
      public void close() throws Exception {
        getCurrentRavenTransactions().pop();
      }
    };
  }

  private static long defaultTimeout;

  public static long getDefaultTimeout() {
    return defaultTimeout;
  }

  public static void setDefaultTimeout(long defaultTimeout) {
    RavenTransactionAccessor.defaultTimeout = defaultTimeout;
  }

  public static TransactionInformation getTransactionInformation() {
    Stack<TransactionInformation> stack = currentRavenTransactions.get();
    if (!stack.isEmpty() && isSupressExplicitRavenTransaction() == false) {
      return currentRavenTransactions.get().peek();
    }
    throw new IllegalStateException("Unable to find transaction");
  }

  public static AutoCloseable supressExplicitRavenTransaction() {
    supressExplicitRavenTransaction.set(true);
    return new AutoCloseable() {

      @Override
      public void close() throws Exception {
        supressExplicitRavenTransaction.set(false);
      }
    };
  }

}

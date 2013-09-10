package raven.querydsl;

import java.util.Stack;

import com.mysema.query.types.Expression;

public class StackBasedContext {

  public StackBasedContext() {
    super();
  }
  public StackBasedContext(StackBasedContext other) {
    super();
    this.expressionStack = other.expressionStack;
  }
  private Stack<Expression<?>> expressionStack = new Stack<>();
  private boolean replace = false;

  public Stack<Expression< ? >> getExpressionStack() {
    return expressionStack;
  }
  public void setExpressionStack(Stack<Expression< ? >> expressionStack) {
    this.expressionStack = expressionStack;
  }
  public boolean isReplace() {
    return replace;
  }
  public void setReplace(boolean replace) {
    this.replace = replace;
  }
  public Expression< ? > push(Expression< ? > item) {
    return expressionStack.push(item);
  }
  public Expression< ? > pop() {
    return expressionStack.pop();
  }
  public void mergeReplace(StackBasedContext c) {
    replace |= c.replace;
  }


}

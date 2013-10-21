package net.ravendb.abstractions.json.linq;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.exceptions.RavenJPathEvaluationException;


public class RavenJPath {

  private String expression;
  private List<Object> parts;
  private int currentIndex;

  public RavenJPath(String expression) {
    this.expression = expression;
    this.parts = new ArrayList<>();
    parseMain();
  }

  private void parseMain() {
    try {
      int currentPartStartIndex = this.currentIndex;
      boolean followingIndexer = false;

      while (this.currentIndex < this.expression.length()) {
        char currentChar = this.expression.charAt(this.currentIndex);

        switch (currentChar) {
        case '[':
        case '(':
          if (this.currentIndex > currentPartStartIndex) {
            String member = this.expression.substring(currentPartStartIndex, this.currentIndex - currentPartStartIndex);
            this.parts.add(member);
          }

          parseIndexer(currentChar);
          currentPartStartIndex = this.currentIndex + 1;
          followingIndexer = true;
          break;
        case ']':
        case ')':
          throw new Exception("Unexpected character while parsing path: " + currentChar);
        case '.':
          if (this.currentIndex > currentPartStartIndex) {
            String member = this.expression.substring(currentPartStartIndex, this.currentIndex - currentPartStartIndex);
            parts.add(member);
          }
          currentPartStartIndex = this.currentIndex + 1;
          followingIndexer = false;
          break;
        default:
          if (followingIndexer) throw new Exception("Unexpected character following indexer: " + currentChar);
          break;
        }

        this.currentIndex++;
      }

      if (this.currentIndex > currentPartStartIndex) {
        String member = this.expression.substring(currentPartStartIndex, this.currentIndex);
        parts.add(member);
      }
    } catch (Exception e) {
      throw new RuntimeException("Unable to evaluate path:" + expression, e);
    }
  }

  private void parseIndexer(char indexerOpenChar) throws Exception {
    this.currentIndex++;

    char indexerCloseChar = (indexerOpenChar == '[') ? ']' : ')';
    int indexerStart = this.currentIndex;
    int indexerLength = 0;
    boolean indexerClosed = false;

    while (this.currentIndex < this.expression.length()) {
      char currentCharacter = this.expression.charAt(this.currentIndex);
      if (Character.isDigit(currentCharacter)) {
        indexerLength++;
      } else if (currentCharacter == indexerCloseChar) {
        indexerClosed = true;
        break;
      } else {
        throw new Exception("Unexpected character while parsing path indexer: " + currentCharacter);
      }

      this.currentIndex++;
    }

    if (!indexerClosed) throw new Exception("Path ended with open indexer. Expected " + indexerCloseChar);

    if (indexerLength == 0) throw new Exception("Empty path indexer.");

    String indexer = this.expression.substring(indexerStart, indexerLength);
    this.parts.add(Integer.parseInt(indexer));
  }

  @SuppressWarnings("null")
  public RavenJToken evaluate(RavenJToken root, boolean errorWhenNoMatch) {
    RavenJToken current = root;

    for (Object part : parts) {
      String propertyName = (String) part;
      if (propertyName != null) {
        RavenJObject o = (RavenJObject) current;
        if (o != null) {
          current = o.get(propertyName);

          if (current == null && errorWhenNoMatch)
            throw new RavenJPathEvaluationException("Property '" + propertyName + "' does not exist on RavenJObject.");
        } else {
          RavenJArray array = (RavenJArray) current;
          if (array != null) {
            switch (propertyName) {
            case "Count":
            case "count":
            case "Length":
            case "length":
            case "Size":
            case "size":
              current = new RavenJValue(array.size());
              break;
            default:
              if (errorWhenNoMatch){
                throw new RavenJPathEvaluationException("Property '" + propertyName + "' not valid on " + current.getType().name() + ".");
              }
              break;
            }
            continue;
          }
          if (errorWhenNoMatch)
            throw new RavenJPathEvaluationException("Property '" + propertyName + "' not valid on " + current.getType().name() + ".");

          return null;
        }
      } else {
        int index = (int) part;

        RavenJArray a = (RavenJArray) current;

        if (a != null) {
          if (a.size() <= index) {
            if (errorWhenNoMatch)
              throw new IndexOutOfBoundsException("Index " + index + " outside the bounds of RavenJArray.");

            return null;
          }

          current = a.get(index);
        } else {
          if (errorWhenNoMatch)
            throw new RavenJPathEvaluationException("Index " + index + " not valid on " + current.getType().name() + ".");

          return null;
        }
      }
    }

    return current;
  }
}
